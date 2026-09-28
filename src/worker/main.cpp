#include <iostream>
#include <string>
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

#ifdef _WIN32
#include <process.h>
#define GET_PID _getpid
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

// Atomically moves every due retry from the delayed set onto the stream.
int promoteDueJobs(redisContext* context) {
    static const char* script = R"LUA(
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 100)
for _, job_id in ipairs(due) do
    redis.call('XADD', KEYS[2], '*', 'job_id', job_id)
    redis.call('ZREM', KEYS[1], job_id)
end
return #due
)LUA";

    redisReply* r = static_cast<redisReply*>(
        redisCommand(
            context,
            "EVAL %s 2 taskflow:delayed taskflow:jobs %lld",
            script,
            nowEpochMs()
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
            "RETURNING id, status, attempts, max_tries",
            stale_ms
        );

        txn.commit();

        for (const auto& row : rows) {
            std::string id = row[0].c_str();
            std::string status = row[1].c_str();
            int attempts = row[2].as<int>();
            int max_tries = row[3].as<int>();

            if (status == "QUEUED") {
                long long delay_ms = computeBackoffMs(attempts);

                redisReply* schedule = static_cast<redisReply*>(
                    redisCommand(
                        context,
                        "ZADD taskflow:delayed %lld %s",
                        nowEpochMs() + delay_ms,
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
                   std::string conninfo)
        : consumer_(std::move(consumer)),
          interval_(interval),
          conninfo_(std::move(conninfo)) {}

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
        ctx_ = redisConnect("127.0.0.1", 6379);
        if (ctx_ == nullptr || ctx_->err) {
            return false;
        }

        try {
            db_ = std::make_unique<pqxx::connection>(conninfo_);
        }
        catch (const std::exception& e) {
            std::cerr << "Lease heartbeat DB connection failed: " << e.what() << "\n";
            return false;
        }

        thread_ = std::thread(&LeaseHeartbeat::run, this);
        return true;
    }

    // Begin renewing the Redis claim on this message.
    void hold(const std::string& message_id) {
        std::lock_guard<std::mutex> lock(mu_);
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
            renew(current_id_);
        }
    }

    // Renews only if this consumer still owns the message, so a worker that
    // lost it can never steal it back from the worker now handling it.
    void renew(const std::string& id) {
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
                "EVAL %s 1 taskflow:jobs taskflow-workers %s %s",
                script,
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
    std::string conninfo_;
    redisContext* ctx_ = nullptr;
    std::unique_ptr<pqxx::connection> db_;
    std::thread thread_;
    std::mutex mu_;
    std::condition_variable cv_;
    std::string current_id_;
    std::string job_id_;
    int attempt_ = 0;
    bool stopping_ = false;
};

// Handles one already-delivered stream message, whether it came from a
// normal XREADGROUP read or was reclaimed by the crash-recovery reaper.
void processMessage(redisContext* context, pqxx::connection& db,
                    LeaseHeartbeat& heartbeat, redisReply* message) {
    const char* message_id = message->element[0]->str;

    heartbeat.hold(message_id);

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
                "RETURNING type, payload, attempts, max_tries",
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

                heartbeat.attach(job_id, my_attempt);

                std::cout << "[t=" << sinceStartMs() << "ms] "
                          << "Job type: " << type
                          << ", payload: " << payload
                          << ", attempt " << my_attempt
                          << "/" << max_tries << "\n";

                std::cout << "Job " << job_id << " set to RUNNING\n";

                bool success = true;

                try {
                    if (type == "email") {
                        // Temporary test hook: hang mid-execution.
                        if (payload.find("crash") != std::string::npos) {
                            std::cout << "Simulating a hang (crash-test payload)... "
                                         "kill this process now to test recovery.\n";
                            std::this_thread::sleep_for(std::chrono::seconds(20));
                        }

                        std::cout << "Simulating: sending email...\n";
                        std::this_thread::sleep_for(std::chrono::milliseconds(500));

                        // Temporary test hook: force a failure.
                        if (payload.find("fail") != std::string::npos) {
                            throw std::runtime_error(
                                "Simulated failure (payload contained 'fail')"
                            );
                        }

                        std::cout << "Email 'sent' (simulated)\n";
                    } else {
                        std::cout << "Unknown job type '" << type
                                  << "', nothing to execute\n";
                    }
                }
                catch (const std::exception& e) {
                    std::cerr << "Job execution failed: " << e.what() << "\n";
                    success = false;
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
                                "ZADD taskflow:delayed %lld %s",
                                due_ms,
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
            "XACK taskflow:jobs taskflow-workers %s",
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
    std::cout << std::unitbuf;
    redisContext* context = redisConnect("127.0.0.1", 6379);

    if (context == nullptr || context->err) {
        std::cerr << "Redis connection failed\n";
        return 1;
    }

    std::cout << "Connected to Redis\n";

    const char* db_password = std::getenv("TASKFLOW_DB_PASSWORD");
    if (db_password == nullptr) {
        std::cerr << "TASKFLOW_DB_PASSWORD environment variable not set\n";
        return 1;
    }

    const std::string conninfo =
        "host=localhost port=5432 dbname=taskflow user=postgres password=" +
        std::string(db_password);

    pqxx::connection db(conninfo);

    std::cout << "Connected to PostgreSQL\n";

    const char* name_env = std::getenv("TASKFLOW_WORKER_NAME");
    std::string consumer_name = name_env != nullptr
        ? std::string(name_env)
        : "worker-" + std::to_string(GET_PID());

    std::cout << "Consumer name: " << consumer_name << "\n";

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
        conninfo
    );

    if (!heartbeat.start()) {
        std::cerr << "Lease heartbeat could not start\n";
        return 1;
    }

    std::cout << "Worker loop started. Waiting for jobs...\n";

    const auto REAP_CHECK_INTERVAL = std::chrono::milliseconds(REAP_CHECK_MS);
    auto last_reap_check = std::chrono::steady_clock::now();

    while (true) {
        int promoted = promoteDueJobs(context);
        if (promoted > 0) {
            std::cout << "[t=" << sinceStartMs() << "ms] Promoter: moved "
                      << promoted << " due retry job(s) onto the stream\n";
        }

        auto now = std::chrono::steady_clock::now();

        if (now - last_reap_check >= REAP_CHECK_INTERVAL) {
            last_reap_check = now;

            // 1) Redis reaper: reclaim messages whose worker stopped renewing.
            redisReply* claim_reply = static_cast<redisReply*>(
                redisCommand(
                    context,
                    "XAUTOCLAIM taskflow:jobs taskflow-workers %s %lld 0",
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
                              << " stuck message(s)\n";

                    for (size_t m = 0; m < claimed->elements; ++m) {
                        processMessage(context, db, heartbeat, claimed->element[m]);
                    }
                }
            }

            if (claim_reply != nullptr) {
                freeReplyObject(claim_reply);
            }

            // 2) Postgres sweeper: recover RUNNING rows Redis can't help with.
            sweepStaleRunningJobs(context, db, SWEEP_STALE_MS);
        }

        redisReply* reply = static_cast<redisReply*>(
            redisCommand(
                context,
                "XREADGROUP GROUP taskflow-workers %s "
                "COUNT 1 BLOCK 1000 STREAMS taskflow:jobs >",
                consumer_name.c_str()
            )
        );

        if (reply == nullptr) {
            std::cerr << "Redis command failed (connection issue). Stopping worker.\n";
            break;
        }

        // An error reply (e.g. NOGROUP because the consumer group is gone)
        // comes back instantly instead of blocking. Without this check the
        // loop below would spin at 100% CPU, hammering Redis.
        if (reply->type == REDIS_REPLY_ERROR) {
            std::cerr << "XREADGROUP error: " << reply->str
                      << " (retrying in 1s)\n";
            freeReplyObject(reply);
            std::this_thread::sleep_for(std::chrono::seconds(1));
            continue;
        }

        if (reply->elements == 0) {
            freeReplyObject(reply);
            continue;
        }

        for (size_t i = 0; i < reply->elements; ++i) {
            redisReply* stream = reply->element[i];
            redisReply* messages = stream->element[1];

            for (size_t j = 0; j < messages->elements; ++j) {
                processMessage(context, db, heartbeat, messages->element[j]);
            }
        }

        freeReplyObject(reply);
    }

    redisFree(context);
    return 0;
}