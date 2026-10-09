#include <iostream>
#include <string>
#include <vector>
#include <array>
#include <sstream>
#include <atomic>
#include <csignal>
#include <cstring>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <memory>
#include <cstdlib>
#include <algorithm>
#include <stdexcept>
#include <hiredis/hiredis.h>
#include <pqxx/pqxx>

#include "../config/Config.h"

#ifdef _WIN32
#include <process.h>
#define GET_PID _getpid
#else
#include <unistd.h>
#define GET_PID getpid
#endif

// --- Backoff settings (hardcoded for now; per-job-type later) ---
const long long BACKOFF_BASE_MS = 2000;
const long long BACKOFF_MAX_MS  = 60000;

const auto WORKER_START = std::chrono::steady_clock::now();

long long sinceStartMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - WORKER_START
    ).count();
}

long long nowEpochMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
}

long long computeBackoffMs(int attempts_made) {
    int shift = std::min(std::max(attempts_made - 1, 0), 20);
    return std::min(BACKOFF_BASE_MS * (1LL << shift), BACKOFF_MAX_MS);
}

// Reads a positive millisecond value from an env var, else the fallback.
long long envMs(const char* name, long long fallback) {
    const char* v = std::getenv(name);
    if (v == nullptr) {
        return fallback;
    }
    try {
        long long parsed = std::stoll(v);
        return parsed > 0 ? parsed : fallback;
    }
    catch (...) {
        return fallback;
    }
}

// ---------------------------------------------------------------------------
// Graceful shutdown.
//   1st signal (Ctrl+C, SIGTERM from `docker stop`, Ctrl+Break on Windows):
//       stop taking new jobs, finish the one in hand, exit with code 0.
//   2nd signal: exit right now. The job in hand is left RUNNING and gets
//       recovered like after a crash (reaper / sweeper).
// ---------------------------------------------------------------------------
std::atomic<int> g_shutdown_signals{0};

void onShutdownSignal(int) {
    // Only async-signal-safe things in here: an atomic counter and _Exit.
    if (g_shutdown_signals.fetch_add(1) >= 1) {
        std::_Exit(130);
    }
}

bool stopRequested() {
    return g_shutdown_signals.load() > 0;
}

// ---------------------------------------------------------------------------
// Priority classes. Each class has its own Redis stream (a stream is strictly
// first-in-first-out, so separate streams are how priorities are expressed).
// The jobs.priority column is 0 = low, 1 = normal, 2 = high.
// ---------------------------------------------------------------------------
const char* CONSUMER_GROUP = "taskflow-workers";
const std::array<const char*, 3> CLASS_NAMES = {"high", "normal", "low"};  // class index 0,1,2

std::string streamFor(int cls) {
    return std::string("taskflow:jobs:") + CLASS_NAMES[cls];
}

int classForPriority(int priority) {
    if (priority >= 2) return 0;   // high
    if (priority <= 0) return 2;   // low
    return 1;                      // normal
}

// "4,2,1" -> {4,2,1}  (high, normal, low). A weight of 0 = never take that class.
bool parseWeights(const char* text, std::array<int, 3>& out) {
    std::stringstream ss(text);
    std::string part;
    int i = 0;
    while (std::getline(ss, part, ',')) {
        if (i >= 3) return false;
        try {
            size_t used = 0;
            int v = std::stoi(part, &used);
            if (used != part.size() || v < 0) return false;
            out[i++] = v;
        }
        catch (...) {
            return false;
        }
    }
    return i == 3;
}

// Smooth weighted round-robin. For weights 4,2,1 this yields the repeating
// cycle  H N H L H N H  : 4 high, 2 normal, 1 low, spread out instead of
// bursty. Classes with weight 0 never appear.
std::vector<int> buildCycle(const std::array<int, 3>& w) {
    int total = 0;
    for (int x : w) total += std::max(x, 0);

    std::vector<int> cycle;
    std::array<int, 3> current = {0, 0, 0};
    for (int step = 0; step < total; ++step) {
        int best = -1;
        for (int i = 0; i < 3; ++i) {
            if (w[i] <= 0) continue;
            current[i] += w[i];
            if (best == -1 || current[i] > current[best]) best = i;
        }
        current[best] -= total;
        cycle.push_back(best);
    }
    return cycle;
}

// Creates the consumer group on every priority stream (and the streams
// themselves) if they don't exist yet. Safe to call from many workers at once.
bool ensureGroups(redisContext* ctx) {
    for (int cls = 0; cls < 3; ++cls) {
        std::string stream = streamFor(cls);
        redisReply* r = static_cast<redisReply*>(
            redisCommand(ctx, "XGROUP CREATE %s %s 0 MKSTREAM", stream.c_str(), CONSUMER_GROUP)
        );
        if (r == nullptr) {
            std::cerr << "Redis connection failed while creating consumer groups\n";
            return false;
        }
        bool bad = (r->type == REDIS_REPLY_ERROR && std::strstr(r->str, "BUSYGROUP") == nullptr);
        if (bad) {
            std::cerr << "XGROUP CREATE " << stream << " failed: " << r->str << "\n";
        }
        freeReplyObject(r);
        if (bad) return false;
    }
    return true;
}

// Reads at most one message per listed class. block_ms = 0 means "don't wait".
// Returns:  1 = messages in `out` (caller frees)   0 = nothing available
//          -1 = Redis error reply (logged)         -2 = connection failure
int readMessages(redisContext* ctx, const std::string& consumer,
                 const std::vector<int>& classes, int block_ms, redisReply*& out) {
    std::vector<std::string> args = {"XREADGROUP", "GROUP", CONSUMER_GROUP, consumer, "COUNT", "1"};
    if (block_ms > 0) {
        args.push_back("BLOCK");
        args.push_back(std::to_string(block_ms));
    }
    args.push_back("STREAMS");
    for (int c : classes) args.push_back(streamFor(c));
    for (size_t i = 0; i < classes.size(); ++i) args.push_back(">");

    std::vector<const char*> argv;
    std::vector<size_t> lens;
    for (const auto& a : args) {
        argv.push_back(a.c_str());
        lens.push_back(a.size());
    }

    redisReply* r = static_cast<redisReply*>(
        redisCommandArgv(ctx, static_cast<int>(argv.size()), argv.data(), lens.data())
    );

    if (r == nullptr) {
        return -2;
    }
    // An error reply (e.g. NOGROUP) comes back instantly instead of blocking;
    // the caller must back off or the loop would spin at 100% CPU.
    if (r->type == REDIS_REPLY_ERROR) {
        std::cerr << "XREADGROUP error: " << r->str << " (retrying in 1s)\n";
        freeReplyObject(r);
        return -1;
    }
    if (r->type != REDIS_REPLY_ARRAY || r->elements == 0) {
        freeReplyObject(r);
        return 0;
    }
    out = r;
    return 1;
}

// Atomically moves every due retry from the delayed set onto its stream.
// Delayed-set members look like "<priority name>|<job id>", e.g. "high|job-...".
int promoteDueJobs(redisContext* context) {
    static const char* script = R"LUA(
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 100)
for _, member in ipairs(due) do
    local sep = string.find(member, '|', 1, true)
    local prio = string.sub(member, 1, sep - 1)
    local job_id = string.sub(member, sep + 1)
    -- The stream key is built here, so this script is for a single Redis
    -- node (not Redis Cluster).
    redis.call('XADD', ARGV[2] .. prio, '*', 'job_id', job_id)
    redis.call('ZREM', KEYS[1], member)
end
return #due
)LUA";

    redisReply* r = static_cast<redisReply*>(
        redisCommand(
            context,
            "EVAL %s 1 taskflow:delayed %lld %s",
            script,
            nowEpochMs(),
            "taskflow:jobs:"
        )
    );

    if (r == nullptr) {
        return -1;
    }

    int moved = (r->type == REDIS_REPLY_INTEGER) ? static_cast<int>(r->integer) : -1;
    freeReplyObject(r);
    return moved;
}

// Finds jobs stuck RUNNING whose worker stopped heartbeating and that no
// Redis message will ever recover (e.g. the message was ACKed but the final
// status write failed). Each one counts as a failed attempt: requeued with
// backoff if tries remain, otherwise marked FAILED. The row locks
// (SKIP LOCKED) make it safe for several workers to sweep at once.
int sweepStaleRunningJobs(redisContext* context, pqxx::connection& db, long long stale_ms) {
    try {
        pqxx::work txn(db);

        pqxx::result rows = txn.exec_params(
            "UPDATE jobs "
            "SET status = CASE WHEN attempts < max_tries THEN 'QUEUED' ELSE 'FAILED' END "
            "WHERE id IN ( "
            "  SELECT id FROM jobs "
            "  WHERE status = 'RUNNING' "
            "    AND (heartbeat_at IS NULL "
            "         OR heartbeat_at < now() - ($1::double precision * interval '1 millisecond')) "
            "  ORDER BY id LIMIT 20 FOR UPDATE SKIP LOCKED "
            ") "
            "RETURNING id, status, attempts, max_tries, priority",
            stale_ms
        );

        txn.commit();

        for (const auto& row : rows) {
            std::string id = row[0].c_str();
            std::string status = row[1].c_str();
            int attempts = row[2].as<int>();
            int max_tries = row[3].as<int>();
            const char* pname = CLASS_NAMES[classForPriority(row[4].as<int>())];

            if (status == "QUEUED") {
                long long delay_ms = computeBackoffMs(attempts);

                redisReply* schedule = static_cast<redisReply*>(
                    redisCommand(
                        context,
                        "ZADD taskflow:delayed %lld %s|%s",
                        nowEpochMs() + delay_ms,
                        pname,
                        id.c_str()
                    )
                );

                if (schedule == nullptr) {
                    // Known gap: QUEUED in Postgres, not scheduled anywhere.
                    std::cerr << "Sweeper: failed to schedule " << id << "\n";
                } else {
                    freeReplyObject(schedule);
                }

                std::cout << "Sweeper: " << id << " was stuck RUNNING (attempt "
                          << attempts << "/" << max_tries
                          << "), requeued, retry in " << delay_ms << "ms\n";
            } else {
                std::cout << "Sweeper: " << id << " was stuck RUNNING and is out of tries ("
                          << attempts << "/" << max_tries << "), marked FAILED\n";

                redisReply* dlq = static_cast<redisReply*>(
                    redisCommand(context, "XADD taskflow:dlq * job_id %s", id.c_str())
                );
                if (dlq == nullptr) {
                    std::cerr << "Failed to record job " << id
                              << " in the dead letter queue\n";
                } else {
                    freeReplyObject(dlq);
                }
            }
        }

        return static_cast<int>(rows.size());
    }
    catch (const std::exception& e) {
        std::cerr << "Sweeper error: " << e.what() << "\n";
        return -1;
    }
}

// Keeps this worker's claim on the message it is processing alive. Runs its
// own thread with its own Redis and Postgres connections (neither hiredis
// nor pqxx connections may be shared across threads). While a message is
// held it (1) resets that message's idle time in the PEL, but only if this
// consumer still owns it, and (2) stamps heartbeat_at on the job row, so the
// reaper and the sweeper both only act on workers that are actually dead.
class LeaseHeartbeat {
public:
    LeaseHeartbeat(std::string consumer,
                   std::chrono::milliseconds interval,
                   config::Settings settings)
        : consumer_(std::move(consumer)),
          interval_(interval),
          settings_(std::move(settings)) {}

    ~LeaseHeartbeat() {
        {
            std::lock_guard<std::mutex> lock(mu_);
            stopping_ = true;
        }
        cv_.notify_all();
        if (thread_.joinable()) {
            thread_.join();
        }
        if (ctx_ != nullptr) {
            redisFree(ctx_);
        }
    }

    bool start() {
        std::string error;
        ctx_ = config::connectRedis(settings_, error);
        if (ctx_ == nullptr) {
            std::cerr << "Lease heartbeat Redis connection failed: " << error << "\n";
            return false;
        }

        try {
            db_ = std::make_unique<pqxx::connection>(settings_.dbConnInfo());
        }
        catch (const std::exception& e) {
            std::cerr << "Lease heartbeat DB connection failed: " << e.what() << "\n";
            return false;
        }

        thread_ = std::thread(&LeaseHeartbeat::run, this);
        return true;
    }

    // Begin renewing the Redis claim on this message.
    void hold(const std::string& stream, const std::string& message_id) {
        std::lock_guard<std::mutex> lock(mu_);
        current_stream_ = stream;
        current_id_ = message_id;
        job_id_.clear();
        attempt_ = 0;
    }

    // Once the job row is claimed, also stamp its heartbeat in Postgres.
    void attach(const std::string& job_id, int attempt) {
        std::lock_guard<std::mutex> lock(mu_);
        job_id_ = job_id;
        attempt_ = attempt;
    }

    // Blocks briefly if a renewal is mid-flight, so no renewal can fire
    // after release() returns.
    void release() {
        std::lock_guard<std::mutex> lock(mu_);
        current_stream_.clear();
        current_id_.clear();
        job_id_.clear();
        attempt_ = 0;
    }

private:
    void run() {
        std::unique_lock<std::mutex> lock(mu_);
        while (!stopping_) {
            cv_.wait_for(lock, interval_, [this] { return stopping_; });
            if (stopping_) {
                break;
            }
            if (current_id_.empty()) {
                continue;
            }
            renew(current_stream_, current_id_);
        }
    }

    // Renews only if this consumer still owns the message, so a worker that
    // lost it can never steal it back from the worker now handling it.
    void renew(const std::string& stream, const std::string& id) {
        static const char* script = R"LUA(
local p = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[3], ARGV[3], 1)
if #p == 0 then return 0 end
if p[1][2] ~= ARGV[2] then return 0 end
redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[2], 0, ARGV[3], 'JUSTID')
return 1
)LUA";

        redisReply* r = static_cast<redisReply*>(
            redisCommand(
                ctx_,
                "EVAL %s 1 %s taskflow-workers %s %s",
                script,
                stream.c_str(),
                consumer_.c_str(),
                id.c_str()
            )
        );

        if (r == nullptr) {
            std::cerr << "[lease] renewal failed (connection issue) for " << id << "\n";
            return;
        }

        if (r->type == REDIS_REPLY_ERROR) {
            std::cerr << "[lease] renewal error: " << r->str << "\n";
        } else if (r->type == REDIS_REPLY_INTEGER && r->integer == 0) {
            std::cerr << "[lease] WARNING: no longer own message " << id
                      << " (another worker reclaimed it)\n";
        } else {
            // Temporary, so the test is observable. Remove once trusted.
            std::cout << "[lease] renewed " << id << "\n";
            renewDb();
        }

        freeReplyObject(r);
    }

    void renewDb() {
        if (job_id_.empty() || !db_) {
            return;
        }
        try {
            pqxx::work w(*db_);
            w.exec_params(
                "UPDATE jobs SET heartbeat_at = now() "
                "WHERE id = $1 AND attempts = $2 AND status = 'RUNNING'",
                job_id_,
                attempt_
            );
            w.commit();
        }
        catch (const std::exception& e) {
            std::cerr << "[lease] DB heartbeat failed: " << e.what() << "\n";
        }
    }

    std::string consumer_;
    std::chrono::milliseconds interval_;
    config::Settings settings_;
    redisContext* ctx_ = nullptr;
    std::unique_ptr<pqxx::connection> db_;
    std::thread thread_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::string current_stream_;
    std::string current_id_;
    std::string job_id_;
    int attempt_ = 0;
    bool stopping_ = false;
};

// Handles one already-delivered stream message, whether it came from a
// normal XREADGROUP read or was reclaimed by the crash-recovery reaper.
void processMessage(redisContext* context, pqxx::connection& db,
                    LeaseHeartbeat& heartbeat, const std::string& stream,
                    redisReply* message) {
    const char* message_id = message->element[0]->str;

    heartbeat.hold(stream, message_id);

    // Set to false if another worker took over this job while we ran it.
    bool owns_message = true;

    std::string job_id;

    redisReply* fields = message->element[1];
    for (size_t k = 0; k < fields->elements; k += 2) {
        std::string key = fields->element[k]->str;
        std::string value = fields->element[k + 1]->str;

        std::cout << "  " << key << " = " << value << "\n";

        if (key == "job_id") {
            job_id = value;
        }
    }

    if (job_id.empty()) {
        std::cerr << "Message " << message_id
                  << " had no job_id field, skipping\n";
    } else {
        std::cout << "Loading job " << job_id << " from PostgreSQL...\n";

        try {
            pqxx::work txn(db);

            // One atomic statement: mark RUNNING, stamp the heartbeat, and
            // take the next attempt number (our fencing token). Jobs that
            // already finished are never picked up again, even if a stale
            // message for them gets reclaimed.
            pqxx::result rows = txn.exec_params(
                "UPDATE jobs "
                "SET status = 'RUNNING', attempts = attempts + 1, heartbeat_at = now() "
                "WHERE id = $1 AND status NOT IN ('COMPLETED', 'FAILED') "
                "RETURNING type, payload, attempts, max_tries, priority",
                job_id
            );

            if (rows.empty()) {
                std::cerr << "Job " << job_id
                          << " is missing or already finished, nothing to run\n";
            } else {
                txn.commit();

                std::string type = rows[0][0].c_str();
                std::string payload = rows[0][1].c_str();
                int my_attempt = rows[0][2].as<int>();
                int max_tries = rows[0][3].as<int>();
                const std::string pname = CLASS_NAMES[classForPriority(rows[0][4].as<int>())];

                heartbeat.attach(job_id, my_attempt);

                std::cout << "[t=" << sinceStartMs() << "ms] "
                          << "Job type: " << type
                          << ", payload: " << payload
                          << ", priority: " << pname
                          << ", attempt " << my_attempt
                          << "/" << max_tries << "\n";

                std::cout << "Job " << job_id << " set to RUNNING\n";

                bool success = true;
                bool claimed_effect = false;

                try {
                    if (type == "email") {
                        // Temporary test hook: hang BEFORE the send, so a crash
                        // here means nothing was ever attempted. Excludes
                        // "crash-after-send" so the two hooks stay independent.
                        if (payload.find("crash") != std::string::npos &&
                            payload.find("crash-after-send") == std::string::npos) {
                            std::cout << "Simulating a hang (crash-test payload)... "
                                         "kill this process now to test recovery.\n";
                            std::this_thread::sleep_for(std::chrono::seconds(20));
                        }

                        // --- Idempotency: claim the right to actually deliver,
                        // exactly once, BEFORE attempting the real action. The
                        // claim happens before, not after, the send so that a
                        // crash right after a successful send still leaves
                        // proof it happened: a later reclaim sees the claim
                        // already taken and skips re-sending. Stated trade-off:
                        // a crash DURING the send also leaves the claim taken,
                        // so that case is treated as "maybe delivered, don't
                        // retry" rather than risking a duplicate — the same
                        // choice real payment/notification systems make when
                        // in doubt.
                        {
                            pqxx::work claim_txn(db);
                            pqxx::result claim_rows = claim_txn.exec_params(
                                "INSERT INTO job_effects (job_id) VALUES ($1) "
                                "ON CONFLICT (job_id) DO NOTHING RETURNING job_id",
                                job_id
                            );
                            claim_txn.commit();
                            claimed_effect = !claim_rows.empty();
                        }

                        if (!claimed_effect) {
                            std::cout << "Job " << job_id
                                      << ": effect already delivered, skipping duplicate send\n";
                        } else {
                            std::cout << "Simulating: sending email...\n";
                            std::this_thread::sleep_for(std::chrono::milliseconds(500));

                            // Temporary test hook: hang AFTER the send. This is
                            // the scenario idempotency exists for — the real
                            // action already happened, then the worker died
                            // before it could say so.
                            if (payload.find("crash-after-send") != std::string::npos) {
                                std::cout << "Simulating a hang after the send "
                                             "(crash-after-send test payload)... "
                                             "kill this process now to test recovery.\n";
                                std::this_thread::sleep_for(std::chrono::seconds(20));
                            }

                            // Temporary test hook: force a failure.
                            if (payload.find("fail") != std::string::npos) {
                                throw std::runtime_error(
                                    "Simulated failure (payload contained 'fail')"
                                );
                            }

                            std::cout << "Email 'sent' (simulated)\n";
                        }
                    } else {
                        std::cout << "Unknown job type '" << type
                                  << "', nothing to execute\n";
                    }
                }
                catch (const std::exception& e) {
                    std::cerr << "Job execution failed: " << e.what() << "\n";
                    success = false;

                    // The send did not genuinely complete, so release the
                    // claim: a retry must be allowed to really attempt
                    // delivery again, not be silently skipped as "already
                    // delivered".
                    if (claimed_effect) {
                        try {
                            pqxx::work release_txn(db);
                            release_txn.exec_params(
                                "DELETE FROM job_effects WHERE job_id = $1", job_id
                            );
                            release_txn.commit();
                        }
                        catch (const std::exception& release_err) {
                            std::cerr << "Failed to release claim for job " << job_id
                                      << ": " << release_err.what() << "\n";
                        }
                    }
                }

                // Fenced write: only succeeds if the job is still RUNNING
                // AND still on the attempt we started.
                auto tryFinish = [&](const char* new_status) -> bool {
                    pqxx::work finish_txn(db);
                    pqxx::result r = finish_txn.exec_params(
                        "UPDATE jobs SET status = $3 "
                        "WHERE id = $1 AND attempts = $2 AND status = 'RUNNING'",
                        job_id,
                        my_attempt,
                        new_status
                    );
                    if (r.affected_rows() == 0) {
                        return false;
                    }
                    finish_txn.commit();
                    return true;
                };

                auto logLost = [&]() {
                    owns_message = false;
                    std::cerr << "Job " << job_id << ": lost ownership of attempt "
                              << my_attempt << " (another worker took over). "
                              << "Discarding this result; not requeueing or ACKing.\n";
                };

                if (success) {
                    if (tryFinish("COMPLETED")) {
                        std::cout << "Job " << job_id << " set to COMPLETED\n";
                    } else {
                        logLost();
                    }
                }
                else if (my_attempt < max_tries) {
                    if (tryFinish("QUEUED")) {
                        long long delay_ms = computeBackoffMs(my_attempt);
                        long long due_ms = nowEpochMs() + delay_ms;

                        redisReply* schedule = static_cast<redisReply*>(
                            redisCommand(
                                context,
                                "ZADD taskflow:delayed %lld %s|%s",
                                due_ms,
                                pname.c_str(),
                                job_id.c_str()
                            )
                        );

                        if (schedule == nullptr) {
                            std::cerr << "Failed to schedule retry for job "
                                      << job_id << "\n";
                        } else {
                            freeReplyObject(schedule);
                        }

                        std::cout << "Job " << job_id << " failed (attempt "
                                  << my_attempt << "/" << max_tries
                                  << "), retry scheduled in " << delay_ms << "ms\n";
                    } else {
                        logLost();
                    }
                }
                else {
                    if (tryFinish("FAILED")) {
                        std::cout << "Job " << job_id
                                  << " permanently FAILED after "
                                  << my_attempt << "/" << max_tries
                                  << " attempts\n";

                        redisReply* dlq = static_cast<redisReply*>(
                            redisCommand(
                                context,
                                "XADD taskflow:dlq * job_id %s",
                                job_id.c_str()
                            )
                        );
                        if (dlq == nullptr) {
                            std::cerr << "Failed to record job " << job_id
                                      << " in the dead letter queue\n";
                        } else {
                            freeReplyObject(dlq);
                        }
                    } else {
                        logLost();
                    }
                }
            }
        }
        catch (const std::exception& e) {
            std::cerr << "PostgreSQL error: " << e.what() << "\n";
        }
    }

    // Stop renewing before we ACK, so no renewal fires on an ACKed message.
    heartbeat.release();

    // If we lost the job to another worker, they own the message now and
    // will ACK it. ACKing here would remove their crash protection.
    if (!owns_message) {
        return;
    }

    redisReply* ack = static_cast<redisReply*>(
        redisCommand(
            context,
            "XACK %s taskflow-workers %s",
            stream.c_str(),
            message_id
        )
    );

    if (ack == nullptr) {
        std::cerr << "XACK failed\n";
    } else {
        std::cout << "XACK result: " << ack->integer << "\n";
        freeReplyObject(ack);
    }
}

int main() {
    // Flush after every log line. Without this, output piped to another
    // process (the test suite, `docker logs`, a file) sits in a buffer.
    std::cout << std::unitbuf;

    std::signal(SIGINT, onShutdownSignal);
    std::signal(SIGTERM, onShutdownSignal);
#ifdef SIGBREAK
    std::signal(SIGBREAK, onShutdownSignal);  // Ctrl+Break / closing the console (Windows)
#endif

    config::Settings settings;
    try {
        settings = config::load();
    }
    catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return 1;
    }

    std::string redis_error;
    redisContext* context = config::connectRedis(settings, redis_error);

    if (context == nullptr) {
        std::cerr << "Redis connection failed: " << redis_error << "\n";
        return 1;
    }

    std::cout << "Connected to Redis (" << settings.redis_host << ":" << settings.redis_port
              << ", db " << settings.redis_db << ")\n";

    std::unique_ptr<pqxx::connection> db_holder;
    try {
        db_holder = std::make_unique<pqxx::connection>(settings.dbConnInfo());
    }
    catch (const std::exception& e) {
        std::cerr << "PostgreSQL connection failed: " << e.what() << "\n";
        return 1;
    }
    pqxx::connection& db = *db_holder;

    std::cout << "Connected to PostgreSQL (" << settings.db_host << ":" << settings.db_port
              << ", database " << settings.db_name << ")\n";

    const char* name_env = std::getenv("TASKFLOW_WORKER_NAME");
    std::string consumer_name;
    if (name_env != nullptr) {
        consumer_name = name_env;
    } else if (const char* hostname_env = std::getenv("HOSTNAME")) {
        // Docker gives every container a unique hostname automatically,
        // including each replica under `docker compose up --scale`. PID
        // alone isn't safe here: a container's main process is almost
        // always PID 1, so every scaled worker would otherwise generate the
        // *same* fallback name and collide as one Redis consumer.
        consumer_name = "worker-" + std::string(hostname_env);
    } else {
        consumer_name = "worker-" + std::to_string(GET_PID());
    }

    std::cout << "Consumer name: " << consumer_name << "\n";

    // --- Priority weights: how often each class gets a turn ---
    // TASKFLOW_WEIGHTS="high,normal,low", default 4,2,1. A 0 means this worker
    // never takes that class (e.g. "0,0,1" = a worker dedicated to low jobs).
    std::array<int, 3> weights = {4, 2, 1};
    const char* weights_env = std::getenv("TASKFLOW_WEIGHTS");
    if (weights_env != nullptr && !parseWeights(weights_env, weights)) {
        std::cerr << "TASKFLOW_WEIGHTS must look like \"4,2,1\" (high,normal,low; "
                     "non-negative integers), got \"" << weights_env << "\"\n";
        return 1;
    }

    const std::vector<int> cycle = buildCycle(weights);
    if (cycle.empty()) {
        std::cerr << "All priority weights are 0: this worker would never take a job\n";
        return 1;
    }

    std::vector<int> allowed;  // classes this worker may take
    for (int cls = 0; cls < 3; ++cls) {
        if (weights[cls] > 0) allowed.push_back(cls);
    }

    std::cout << "Priority weights: high=" << weights[0] << " normal=" << weights[1]
              << " low=" << weights[2] << "\n";

    if (!ensureGroups(context)) {
        return 1;
    }

    // --- Failure-recovery timing ---
    // With a lease, live workers keep renewing, so the idle timeout only has
    // to outlast a few missed renewals, not a whole job. Defaults suit real
    // use; override per terminal to make tests fast, e.g.
    //   $env:TASKFLOW_REAP_IDLE_MS = "6000"
    //   $env:TASKFLOW_REAP_CHECK_MS = "2000"
    const long long REAP_IDLE_MS   = envMs("TASKFLOW_REAP_IDLE_MS", 60000);
    const long long REAP_CHECK_MS  = envMs("TASKFLOW_REAP_CHECK_MS", 10000);
    // The DB sweeper waits twice as long, so the Redis reaper always gets
    // first shot at a dead worker's message.
    const long long SWEEP_STALE_MS = REAP_IDLE_MS * 2;

    std::cout << "Recovery config: reap idle " << REAP_IDLE_MS
              << "ms, check every " << REAP_CHECK_MS
              << "ms, sweep stale after " << SWEEP_STALE_MS << "ms\n";

    // Renew at a third of the idle timeout so one or two missed renewals
    // don't get a healthy worker's job reclaimed.
    LeaseHeartbeat heartbeat(
        consumer_name,
        std::chrono::milliseconds(REAP_IDLE_MS / 3),
        settings
    );

    if (!heartbeat.start()) {
        std::cerr << "Lease heartbeat could not start\n";
        return 1;
    }

    std::cout << "Worker loop started. Waiting for jobs...\n";

    const auto REAP_CHECK_INTERVAL = std::chrono::milliseconds(REAP_CHECK_MS);
    auto last_reap_check = std::chrono::steady_clock::now();

    size_t pos = 0;  // where in the weighted cycle we are
    int exit_code = 0;

    while (true) {
        // Checked once per pass, so a job already in hand is always finished.
        if (stopRequested()) {
            std::cout << "Shutdown requested: not taking new jobs.\n";
            break;
        }

        int promoted = promoteDueJobs(context);
        if (promoted > 0) {
            std::cout << "[t=" << sinceStartMs() << "ms] Promoter: moved "
                      << promoted << " due retry job(s) onto their streams\n";
        }

        auto now = std::chrono::steady_clock::now();

        if (now - last_reap_check >= REAP_CHECK_INTERVAL) {
            last_reap_check = now;

            // 1) Redis reaper: for each class this worker serves, reclaim
            //    messages whose worker stopped renewing.
            for (int cls : allowed) {
                if (stopRequested()) break;
                std::string stream = streamFor(cls);

                redisReply* claim_reply = static_cast<redisReply*>(
                    redisCommand(
                        context,
                        "XAUTOCLAIM %s taskflow-workers %s %lld 0",
                        stream.c_str(),
                        consumer_name.c_str(),
                        REAP_IDLE_MS
                    )
                );

                if (claim_reply != nullptr &&
                    claim_reply->type == REDIS_REPLY_ARRAY &&
                    claim_reply->elements >= 2) {

                    redisReply* claimed = claim_reply->element[1];

                    if (claimed->elements > 0) {
                        std::cout << "Reaper: reclaimed " << claimed->elements
                                  << " stuck message(s) from " << stream << "\n";

                        for (size_t m = 0; m < claimed->elements; ++m) {
                            // Anything left over stays claimed by us, and another
                            // worker's reaper picks it up after the idle timeout.
                            if (stopRequested()) break;
                            processMessage(context, db, heartbeat, stream, claimed->element[m]);
                        }
                    }
                }

                if (claim_reply != nullptr) {
                    freeReplyObject(claim_reply);
                }
            }

            // 2) Postgres sweeper: recover RUNNING rows Redis can't help with.
            sweepStaleRunningJobs(context, db, SWEEP_STALE_MS);
        }

        redisReply* reply = nullptr;
        int status = 0;

        // Pass 1: walk the weighted cycle from where we left off and take a
        // job from the first class that has one waiting. Empty classes are
        // skipped, so no capacity is wasted.
        std::array<bool, 3> tried = {false, false, false};
        size_t taken_at = 0;
        for (size_t k = 0; k < cycle.size(); ++k) {
            size_t idx = (pos + k) % cycle.size();
            int cls = cycle[idx];
            if (tried[cls]) continue;
            tried[cls] = true;

            status = readMessages(context, consumer_name, {cls}, 0, reply);
            if (status != 0) {
                taken_at = idx;
                break;
            }
        }

        if (status == 1) {
            pos = (taken_at + 1) % cycle.size();
        }
        else if (status == 0) {
            // Pass 2: nothing is waiting anywhere. Block until a job arrives
            // in any class we serve (up to 1s, so the promoter and reaper
            // keep running).
            status = readMessages(context, consumer_name, allowed, 1000, reply);
        }

        if (status == -2) {
            std::cerr << "Redis command failed (connection issue). Stopping worker.\n";
            exit_code = 1;
            break;
        }
        if (status == -1) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }
        if (status == 0) {
            continue;
        }

        for (size_t i = 0; i < reply->elements; ++i) {
            redisReply* stream_reply = reply->element[i];
            std::string stream_name = stream_reply->element[0]->str;
            redisReply* messages = stream_reply->element[1];

            for (size_t j = 0; j < messages->elements; ++j) {
                processMessage(context, db, heartbeat, stream_name, messages->element[j]);
            }
        }

        freeReplyObject(reply);
    }

    if (stopRequested()) {
        std::cout << "Worker stopped cleanly.\n";
    }

    redisFree(context);
    return exit_code;
}