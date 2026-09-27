#include <drogon/drogon.h>
#include <pqxx/pqxx>
#include <hiredis/hiredis.h>

#include "../common/models/Job.h"

using namespace std;

string generateJobId() {
    return "job-" + drogon::utils::getUuid();
}

int main() {

    // Connect to PostgreSQL once when the API starts
    const char* db_password = std::getenv("TASKFLOW_DB_PASSWORD");
    if (db_password == nullptr) {
        std::cerr << "TASKFLOW_DB_PASSWORD environment variable not set\n";
        return 1;
    }

    pqxx::connection db(
        "host=localhost port=5432 dbname=taskflow user=postgres password=" +
        std::string(db_password)
    );

    // Connect to Redis once when the API starts
    redisContext* redis = redisConnect("127.0.0.1", 6379);

    if (redis == nullptr || redis->err) {
        std::cerr << "Redis connection failed\n";
        return 1;
    }

    std::cout << "Connected to Redis\n";

    drogon::app().registerHandler(
        "/jobs",

        [&db, redis](const drogon::HttpRequestPtr& req,
              std::function<void(
                  const drogon::HttpResponsePtr&)>&& callback) {

            auto json = req->getJsonObject();

            if (!json) {
                auto response =
                    drogon::HttpResponse::newHttpJsonResponse(
                        Json::Value("Invalid JSON")
                    );

                response->setStatusCode(
                    drogon::k400BadRequest
                );

                callback(response);
                return;
            }

            Job job;

            job.id = generateJobId();
            job.type = (*json)["type"].asString();
            job.payload = (*json)["payload"].asString();

            int priority = (*json)["priority"].asInt();

            if (priority == 0) {
                job.priority = JobPriority::LOW;
            }
            else if (priority == 2) {
                job.priority = JobPriority::HIGH;
            }
            else {
                job.priority = JobPriority::NORMAL;
            }

            job.status = JobStatus::QUEUED;

            try {
                pqxx::work transaction(db);

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

                transaction.commit();
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

            // Job is safely in PostgreSQL. Now enqueue its ID into
            // Redis so a worker picks it up.
            redisReply* reply = static_cast<redisReply*>(
                redisCommand(
                    redis,
                    "XADD taskflow:jobs * job_id %s",
                    job.id.c_str()
                )
            );

            if (reply == nullptr) {
                // Known gap: job exists in Postgres but was never
                // queued. Not solving this yet — see note above.
                std::cerr << "Failed to enqueue job " << job.id
                          << " into Redis\n";
            } else {
                freeReplyObject(reply);
            }

            Json::Value responseJson;

            responseJson["id"] = job.id;
            responseJson["status"] = "QUEUED";

            auto response =
                drogon::HttpResponse::newHttpJsonResponse(
                    responseJson
                );

            callback(response);
        },

        {drogon::Post}
    );

    drogon::app()
        .addListener("0.0.0.0", 8080)
        .run();

    redisFree(redis);

    return 0;
}