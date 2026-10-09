#include <drogon/drogon.h>
#include <pqxx/pqxx>
#include <hiredis/hiredis.h>
#include <cmath>
#include <memory>

#include "../common/models/Job.h"
#include "../config/Config.h"

using namespace std;

string generateJobId() {
    return "job-" + drogon::utils::getUuid();
}

// How a job is scheduled: right away, after a delay, or at a given moment.
enum class Schedule { NONE, DELAY, RUN_AT };

int main() {
    // Flush after every log line. Without this, output piped to another
    // process (docker logs, a file, a test harness) sits in a buffer and may
    // never become visible while the process keeps running.
    std::cout << std::unitbuf;

    config::Settings settings;
    try {
        settings = config::load();
    }
    catch (const std::exception& e) {
        std::cerr << e.what() << "\n";
        return 1;
    }

    // Connect to PostgreSQL once when the API starts
    std::unique_ptr<pqxx::connection> db_holder;
    try {
        db_holder = std::make_unique<pqxx::connection>(settings.dbConnInfo());
    }
    catch (const std::exception& e) {
        std::cerr << "PostgreSQL connection failed: " << e.what() << "\n";
        return 1;
    }
    pqxx::connection& db = *db_holder;

    // Connect to Redis once when the API starts
    std::string redis_error;
    redisContext* redis = config::connectRedis(settings, redis_error);

    if (redis == nullptr) {
        std::cerr << "Redis connection failed: " << redis_error << "\n";
        return 1;
    }

    std::cout << "Connected to Redis (" << settings.redis_host << ":" << settings.redis_port
              << ", db " << settings.redis_db << ")\n";

    drogon::app().registerHandler(
        "/jobs",

        [&db, redis](const drogon::HttpRequestPtr& req,
              std::function<void(
                  const drogon::HttpResponsePtr&)>&& callback) {

            // Replies 400 with {"error": "..."}.
            auto badRequest = [&callback](const std::string& message) {
                Json::Value body;
                body["error"] = message;

                auto response = drogon::HttpResponse::newHttpJsonResponse(body);
                response->setStatusCode(drogon::k400BadRequest);
                callback(response);
            };

            auto json = req->getJsonObject();

            if (!json) {
                badRequest("Invalid JSON");
                return;
            }

            Job job;
            Schedule schedule = Schedule::NONE;
            std::string schedule_value;  // seconds (DELAY) or a timestamp (RUN_AT)

            try {
                job.id = generateJobId();
                job.type = (*json)["type"].asString();
                job.payload = (*json)["payload"].asString();

                // Priority is optional and defaults to NORMAL (1).
                int priority = 1;
                if (json->isMember("priority")) {
                    if (!(*json)["priority"].isIntegral()) {
                        badRequest("priority must be 0 (low), 1 (normal) or 2 (high)");
                        return;
                    }
                    priority = (*json)["priority"].asInt();
                }

                if (priority == 0) {
                    job.priority = JobPriority::LOW;
                }
                else if (priority == 2) {
                    job.priority = JobPriority::HIGH;
                }
                else {
                    job.priority = JobPriority::NORMAL;
                }

                // Scheduling is optional: "delay_seconds" or "run_at", not both.
                const bool has_delay = json->isMember("delay_seconds");
                const bool has_run_at = json->isMember("run_at");

                if (has_delay && has_run_at) {
                    badRequest("use either delay_seconds or run_at, not both");
                    return;
                }

                if (has_delay) {
                    const Json::Value& delay = (*json)["delay_seconds"];
                    // isNumeric() is false for booleans, strings and null.
                    if (!delay.isNumeric() || !std::isfinite(delay.asDouble()) ||
                        delay.asDouble() < 0 || delay.asDouble() > 3153600000.0) {
                        badRequest("delay_seconds must be a number between 0 and 3153600000");
                        return;
                    }
                    schedule = Schedule::DELAY;
                    schedule_value = std::to_string(delay.asDouble());
                }
                else if (has_run_at) {
                    const Json::Value& run_at = (*json)["run_at"];
                    if (!run_at.isString() || run_at.asString().empty()) {
                        badRequest("run_at must be a timestamp string such as 2026-10-01T15:00:00Z");
                        return;
                    }
                    schedule = Schedule::RUN_AT;
                    schedule_value = run_at.asString();
                }
            }
            catch (const std::exception& e) {
                // e.g. "payload" was an object, or "type" a list
                badRequest(std::string("Malformed job: ") + e.what());
                return;
            }

            job.status = JobStatus::QUEUED;

            long long run_at_ms = 0;  // when a scheduled job is due (epoch milliseconds)

            try {
                pqxx::work transaction(db);

                if (schedule == Schedule::NONE) {
                    transaction.exec_params(
                        "INSERT INTO jobs "
                        "(id, type, payload, status, priority) "
                        "VALUES ($1, $2, $3, $4, $5)",
                        job.id,
                        job.type,
                        job.payload,
                        "QUEUED",
                        static_cast<int>(job.priority)
                    );
                }
                else {
                    // PostgreSQL parses the timestamp and does the date math,
                    // and hands back the due time in epoch milliseconds.
                    const std::string run_at_sql =
                        schedule == Schedule::DELAY
                            ? "now() + make_interval(secs => $6::double precision)"
                            : "$6::timestamptz";

                    pqxx::result inserted = transaction.exec_params(
                        "INSERT INTO jobs "
                        "(id, type, payload, status, priority, run_at) "
                        "VALUES ($1, $2, $3, $4, $5, " + run_at_sql + ") "
                        "RETURNING (extract(epoch from run_at) * 1000)::bigint",
                        job.id,
                        job.type,
                        job.payload,
                        "QUEUED",
                        static_cast<int>(job.priority),
                        schedule_value
                    );

                    run_at_ms = inserted[0][0].as<long long>();
                }

                transaction.commit();
            }
            catch (const pqxx::data_exception& e) {
                // PostgreSQL could not make sense of run_at / delay_seconds.
                badRequest(std::string("Invalid schedule: ") + e.what());
                return;
            }
            catch (const std::exception& e) {

                auto response =
                    drogon::HttpResponse::newHttpResponse();

                response->setStatusCode(
                    drogon::k500InternalServerError
                );

                response->setBody(
                    "Database error: " + string(e.what())
                );

                callback(response);
                return;
            }

            // Job is safely in PostgreSQL. Now hand it to Redis, in the queue
            // that matches its priority.
            const char* priority_name =
                job.priority == JobPriority::HIGH ? "high" :
                job.priority == JobPriority::LOW  ? "low"  :
                                                    "normal";

            redisReply* reply = nullptr;

            if (schedule == Schedule::NONE) {
                // Run as soon as a worker is free.
                reply = static_cast<redisReply*>(
                    redisCommand(
                        redis,
                        "XADD taskflow:jobs:%s * job_id %s",
                        priority_name,
                        job.id.c_str()
                    )
                );
            }
            else {
                // Park it in the delayed set, scored with its due time. Workers
                // move it onto the queue when the time comes. The entry is
                // "<priority name>|<job id>" so the worker knows which queue.
                reply = static_cast<redisReply*>(
                    redisCommand(
                        redis,
                        "ZADD taskflow:delayed %lld %s|%s",
                        run_at_ms,
                        priority_name,
                        job.id.c_str()
                    )
                );
            }

            if (reply == nullptr) {
                // Known gap: job exists in Postgres but was never
                // queued. Not solving this yet.
                std::cerr << "Failed to enqueue job " << job.id
                          << " into Redis\n";
            } else {
                freeReplyObject(reply);
            }

            Json::Value responseJson;

            responseJson["id"] = job.id;
            responseJson["status"] = "QUEUED";

            if (schedule != Schedule::NONE) {
                responseJson["run_at_ms"] = static_cast<Json::Int64>(run_at_ms);
            }

            auto response =
                drogon::HttpResponse::newHttpJsonResponse(
                    responseJson
                );

            callback(response);
        },

        {drogon::Post}
    );

    // Dead-letter redrive: give a permanently-FAILED job a fresh attempt
    // budget and put it back on its original priority queue. {1} binds to
    // the jobId parameter below (Drogon's positional path-parameter syntax).
    drogon::app().registerHandler(
        "/jobs/{1}/redrive",

        [&db, redis](const drogon::HttpRequestPtr& req,
              std::function<void(
                  const drogon::HttpResponsePtr&)>&& callback,
              const std::string& jobId) {

            auto notFound = [&callback](const std::string& message) {
                Json::Value body;
                body["error"] = message;

                auto response = drogon::HttpResponse::newHttpJsonResponse(body);
                response->setStatusCode(drogon::k404NotFound);
                callback(response);
            };

            int priority = 1;

            try {
                pqxx::work transaction(db);

                // Only a job that is actually dead (FAILED) can be redriven;
                // this also atomically doubles as the "does it exist" check.
                pqxx::result rows = transaction.exec_params(
                    "UPDATE jobs SET status = 'QUEUED', attempts = 0 "
                    "WHERE id = $1 AND status = 'FAILED' "
                    "RETURNING priority",
                    jobId
                );

                if (rows.empty()) {
                    notFound("job not found, or not in a FAILED state");
                    return;
                }

                transaction.commit();
                priority = rows[0][0].as<int>();
            }
            catch (const std::exception& e) {
                auto response = drogon::HttpResponse::newHttpResponse();
                response->setStatusCode(drogon::k500InternalServerError);
                response->setBody("Database error: " + std::string(e.what()));
                callback(response);
                return;
            }

            // Clear any leftover idempotency claim so a manual redrive gets a
            // genuine fresh delivery attempt, not a silent skip. In normal
            // operation a FAILED job's claim was already released on its
            // last failed attempt; this is defensive cleanup, not the usual
            // path.
            try {
                pqxx::work claimTxn(db);
                claimTxn.exec_params(
                    "DELETE FROM job_effects WHERE job_id = $1", jobId
                );
                claimTxn.commit();
            }
            catch (const std::exception& e) {
                std::cerr << "Failed to clear idempotency claim for " << jobId
                          << " during redrive: " << e.what() << "\n";
            }

            const char* priorityName =
                priority == 2 ? "high" :
                priority == 0 ? "low"  :
                                "normal";

            redisReply* reply = static_cast<redisReply*>(
                redisCommand(
                    redis,
                    "XADD taskflow:jobs:%s * job_id %s",
                    priorityName,
                    jobId.c_str()
                )
            );

            if (reply == nullptr) {
                std::cerr << "Failed to re-enqueue redriven job " << jobId << "\n";
            } else {
                freeReplyObject(reply);
            }

            Json::Value responseJson;
            responseJson["id"] = jobId;
            responseJson["status"] = "QUEUED";

            callback(drogon::HttpResponse::newHttpJsonResponse(responseJson));
        },

        {drogon::Post}
    );

    drogon::app()
        .addListener("0.0.0.0", settings.api_port)
        .run();

    redisFree(redis);

    return 0;
}