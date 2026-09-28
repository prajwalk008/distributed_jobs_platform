#include <iostream>
#include <string>
#include <chrono>
#include <thread>
#include <cstdlib>
#include <hiredis/hiredis.h>
#include <pqxx/pqxx>
#ifdef _WIN32
#include <process.h>
#define GET_PID _getpid
#endif

// Handles one already-delivered stream message, regardless of whether it
// came from a normal XREADGROUP read or was reclaimed by the crash-recovery
// reaper below. Keeping this in one place means a reclaimed job goes
// through the exact same attempt-tracking / retry logic as any other job.
void processMessage(redisContext* context, pqxx::connection& db, redisReply* message) {
    const char* message_id = message->element[0]->str;

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

            pqxx::result rows = txn.exec_params(
                "SELECT type, payload, attempts, max_tries "
                "FROM jobs WHERE id = $1",
                job_id
            );

            if (rows.empty()) {
                std::cerr << "No job found in PostgreSQL with id "
                          << job_id << "\n";
            } else {
                std::string type = rows[0][0].c_str();
                std::string payload = rows[0][1].c_str();
                int attempts = rows[0][2].as<int>();
                int max_tries = rows[0][3].as<int>();

                int new_attempts = attempts + 1;

                std::cout << "Job type: " << type
                          << ", payload: " << payload
                          << ", attempt " << new_attempts
                          << "/" << max_tries << "\n";

                txn.exec_params(
                    "UPDATE jobs SET status = 'RUNNING', attempts = $2 "
                    "WHERE id = $1",
                    job_id,
                    new_attempts
                );

                txn.commit();

                std::cout << "Job " << job_id << " set to RUNNING\n";

                bool success = true;

                try {
                    if (type == "email") {
                        // Temporary test hook: hang mid-execution so you can
                        // manually kill the process and simulate a crash.
                        // Remove once there's a real way to induce a hang.
                        if (payload.find("crash") != std::string::npos) {
                            std::cout << "Simulating a hang (crash-test payload)... "
                                         "kill this process now to test recovery.\n";
                            std::this_thread::sleep_for(std::chrono::seconds(20));
                        }

                        std::cout << "Simulating: sending email...\n";
                        std::this_thread::sleep_for(std::chrono::milliseconds(500));

                        // Temporary test hook: force a failure if the payload
                        // contains "fail", so retry logic is testable. Remove
                        // once real job types can genuinely fail on their own.
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

                pqxx::work finish_txn(db);

                if (success) {
                    finish_txn.exec_params(
                        "UPDATE jobs SET status = 'COMPLETED' WHERE id = $1",
                        job_id
                    );
                    finish_txn.commit();

                    std::cout << "Job " << job_id << " set to COMPLETED\n";
                }
                else if (new_attempts < max_tries) {
                    finish_txn.exec_params(
                        "UPDATE jobs SET status = 'QUEUED' WHERE id = $1",
                        job_id
                    );
                    finish_txn.commit();

                    redisReply* requeue = static_cast<redisReply*>(
                        redisCommand(
                            context,
                            "XADD taskflow:jobs * job_id %s",
                            job_id.c_str()
                        )
                    );

                    if (requeue == nullptr) {
                        std::cerr << "Failed to requeue job " << job_id << "\n";
                    } else {
                        freeReplyObject(requeue);
                    }

                    std::cout << "Job " << job_id << " failed (attempt "
                              << new_attempts << "/" << max_tries
                              << "), requeued\n";
                }
                else {
                    finish_txn.exec_params(
                        "UPDATE jobs SET status = 'FAILED' WHERE id = $1",
                        job_id
                    );
                    finish_txn.commit();

                    std::cout << "Job " << job_id
                              << " permanently FAILED after "
                              << new_attempts << "/" << max_tries
                              << " attempts\n";
                }
            }
        }
        catch (const std::exception& e) {
            std::cerr << "PostgreSQL error: " << e.what() << "\n";
        }
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

    pqxx::connection db(
        "host=localhost port=5432 dbname=taskflow user=postgres password=" +
        std::string(db_password)
    );

    std::cout << "Connected to PostgreSQL\n";

    const char* name_env = std::getenv("TASKFLOW_WORKER_NAME");
    std::string consumer_name = name_env != nullptr
        ? std::string(name_env)
        : "worker-" + std::to_string(GET_PID());

    std::cout << "Consumer name: " << consumer_name << "\n";

    std::cout << "Worker loop started. Waiting for jobs...\n";

    // --- Crash recovery (XAUTOCLAIM) settings ---
    // How often we check the PEL for stuck messages.
    const auto REAP_CHECK_INTERVAL = std::chrono::seconds(30);
    // How long a message must sit unACKed before we treat its worker as
    // dead and reclaim it. 15s here is a TEST value so you don't have to
    // wait 5+ real minutes; a real deployment would use something like
    // 5-10 minutes, as originally discussed.
    const int REAP_MIN_IDLE_MS = 15 * 1000;

    auto last_reap_check = std::chrono::steady_clock::now();

    while (true) {
        auto now = std::chrono::steady_clock::now();

        if (now - last_reap_check >= REAP_CHECK_INTERVAL) {
            last_reap_check = now;

            redisReply* claim_reply = static_cast<redisReply*>(
                redisCommand(
                    context,
                    "XAUTOCLAIM taskflow:jobs taskflow-workers %s %d 0",
                    consumer_name.c_str(),
                    REAP_MIN_IDLE_MS
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
                        processMessage(context, db, claimed->element[m]);
                    }
                }
            }

            if (claim_reply != nullptr) {
                freeReplyObject(claim_reply);
            }
        }

        redisReply* reply = static_cast<redisReply*>(
            redisCommand(
                context,
                "XREADGROUP GROUP taskflow-workers %s "
                "COUNT 1 BLOCK 5000 STREAMS taskflow:jobs >",
                consumer_name.c_str()
            )
        );

        if (reply == nullptr) {
            std::cerr << "Redis command failed (connection issue). Stopping worker.\n";
            break;
        }

        if (reply->elements == 0) {
            freeReplyObject(reply);
            continue;
        }

        for (size_t i = 0; i < reply->elements; ++i) {
            redisReply* stream = reply->element[i];
            redisReply* messages = stream->element[1];

            for (size_t j = 0; j < messages->elements; ++j) {
                processMessage(context, db, messages->element[j]);
            }
        }

        freeReplyObject(reply);
    }

    redisFree(context);
    return 0;
}