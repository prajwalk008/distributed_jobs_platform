#include <iostream>
#include <string>
#include <hiredis/hiredis.h>
#include <pqxx/pqxx>
#include <thread>
#include <chrono>

using namespace std;

int main() {
    redisContext* context = redisConnect("127.0.0.1", 6379); // this is the connection object.

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

    cout << "Connected to PostgreSQL\n";
    cout << "Worker loop started. Waiting for jobs...\n";

    while (true) {
        redisReply* reply = static_cast<redisReply*>(
            redisCommand(
                context,
                "XREADGROUP GROUP taskflow-workers worker-1 "
                "COUNT 1 BLOCK 5000 STREAMS taskflow:jobs >"
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
                redisReply* message = messages->element[j];
                const char* message_id = message->element[0]->str;

                string job_id;

                redisReply* fields = message->element[1];
                for (size_t k = 0; k < fields->elements; k += 2) {
                    string key = fields->element[k]->str;
                    string value = fields->element[k + 1]->str;

                    cout << "  " << key << " = " << value << "\n";

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
                            "SELECT type, payload FROM jobs WHERE id = $1",
                            job_id
                        );

                        if (rows.empty()) {
                            cerr << "No job found in PostgreSQL with id "
                                      << job_id << "\n";
                        } else {
                            string type = rows[0][0].c_str();
                            string payload = rows[0][1].c_str();

                            cout << "Job type: " << type
                                      << ", payload: " << payload << "\n";

                            txn.exec_params(
                                "UPDATE jobs SET status = 'RUNNING' WHERE id = $1",
                                job_id
                            );

                            txn.commit();


                            std::cout << "Job " << job_id << " set to RUNNING\n";

                            // --- Execute the job (toy implementation) ---
                            bool success = true;

                            try {
                                if (type == "email") {
                                    std::cout << "Simulating: sending email...\n";
                                    std::this_thread::sleep_for(std::chrono::milliseconds(500));
                                    std::cout << "Email 'sent' (simulated)\n";
                                } else {
                                    std::cout << "Unknown job type '" << type
                                              << "', nothing to execute\n";
                                }
                            }
                            catch (const std::exception& e) {
                                std::cerr << "Job execution threw: " << e.what() << "\n";
                                success = false;
                            }

                            // --- Update final status ---
                            pqxx::work finish_txn(db);

                            finish_txn.exec_params(
                                "UPDATE jobs SET status = $2 WHERE id = $1",
                                job_id,
                                success ? "COMPLETED" : "FAILED"
                            );

                            finish_txn.commit();

                            std::cout << "Job " << job_id << " set to "
                                      << (success ? "COMPLETED" : "FAILED") << "\n";
                        }
                    }
                    catch (const std::exception& e) {
                        std::cerr << "PostgreSQL error: " << e.what() << "\n";
                    }
                }

                // Still ACKing unconditionally here — success/failure-based
                // ACK timing is part of the next sub-step, not this one.
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
        }

        freeReplyObject(reply);
    }

    redisFree(context); // deleting the connection object
    return 0;
}