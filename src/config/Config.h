#pragma once

// Where the services live and how to reach them.
//
// Every value can be overridden with an environment variable. The defaults
// match a plain local setup (everything on this machine), so nothing has to
// be set to run locally, except the database password.
//
//   TASKFLOW_REDIS_HOST   127.0.0.1
//   TASKFLOW_REDIS_PORT   6379
//   TASKFLOW_REDIS_DB     0            (Redis "logical database" number)
//   TASKFLOW_DB_HOST      localhost
//   TASKFLOW_DB_PORT      5432
//   TASKFLOW_DB_NAME      taskflow
//   TASKFLOW_DB_USER      postgres
//   TASKFLOW_DB_PASSWORD  (required, no default)
//   TASKFLOW_API_PORT     8080
//
// Inside Docker Compose the hosts become the service names, e.g.
// TASKFLOW_REDIS_HOST=redis and TASKFLOW_DB_HOST=postgres.

#ifdef _WIN32
// On MSVC, hiredis.h only forward-declares "struct timeval" (it expects the
// application to bring in the real definition) instead of defining it, so
// redisConnectWithTimeout() below won't compile without this. Must come
// before anything in this translation unit could pull in plain <windows.h>,
// so it's the very first Windows-specific include.
//   WIN32_LEAN_AND_MEAN trims rarely-used Windows APIs out of windows.h.
//   NOMINMAX stops windows.h from #define-ing min/max as macros, which
//   would otherwise silently break every std::min / std::max call in any
//   file that includes this header.
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#endif

#include <cstdlib>
#include <stdexcept>
#include <string>

#include <hiredis/hiredis.h>

namespace config {

// An unset or empty variable means "use the default".
inline std::string envString(const char* name, const std::string& fallback) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }
    return value;
}

// A whole number within [min, max]; anything else is a clear error.
inline int envInt(const char* name, int fallback, int min, int max) {
    const char* value = std::getenv(name);
    if (value == nullptr || *value == '\0') {
        return fallback;
    }

    try {
        size_t used = 0;
        int parsed = std::stoi(value, &used);
        if (used == std::string(value).size() && parsed >= min && parsed <= max) {
            return parsed;
        }
    }
    catch (...) {
        // fall through to the error below
    }

    throw std::runtime_error(
        std::string(name) + " must be a whole number between " + std::to_string(min) +
        " and " + std::to_string(max) + ", got \"" + value + "\""
    );
}

// libpq connection strings need single quotes and backslashes escaped, so a
// password containing a space or a quote doesn't break the connection.
inline std::string quoteForLibpq(const std::string& value) {
    std::string out = "'";
    for (char c : value) {
        if (c == '\\' || c == '\'') {
            out += '\\';
        }
        out += c;
    }
    out += "'";
    return out;
}

struct Settings {
    std::string redis_host = "127.0.0.1";
    int redis_port = 6379;
    int redis_db = 0;

    std::string db_host = "localhost";
    int db_port = 5432;
    std::string db_name = "taskflow";
    std::string db_user = "postgres";
    std::string db_password;

    int api_port = 8080;

    // PostgreSQL connection string for pqxx::connection.
    std::string dbConnInfo() const {
        return "host=" + quoteForLibpq(db_host) +
               " port=" + std::to_string(db_port) +
               " dbname=" + quoteForLibpq(db_name) +
               " user=" + quoteForLibpq(db_user) +
               " password=" + quoteForLibpq(db_password);
    }
};

// Reads all settings from the environment. Throws std::runtime_error with a
// message naming the offending variable.
inline Settings load() {
    Settings s;

    s.redis_host = envString("TASKFLOW_REDIS_HOST", s.redis_host);
    s.redis_port = envInt("TASKFLOW_REDIS_PORT", s.redis_port, 1, 65535);
    s.redis_db = envInt("TASKFLOW_REDIS_DB", s.redis_db, 0, 255);

    s.db_host = envString("TASKFLOW_DB_HOST", s.db_host);
    s.db_port = envInt("TASKFLOW_DB_PORT", s.db_port, 1, 65535);
    s.db_name = envString("TASKFLOW_DB_NAME", s.db_name);
    s.db_user = envString("TASKFLOW_DB_USER", s.db_user);
    s.db_password = envString("TASKFLOW_DB_PASSWORD", "");

    s.api_port = envInt("TASKFLOW_API_PORT", s.api_port, 1, 65535);

    if (s.db_password.empty()) {
        throw std::runtime_error("TASKFLOW_DB_PASSWORD environment variable not set");
    }

    return s;
}

// Connects to Redis and selects the configured logical database. Returns
// nullptr and fills `error` on failure. The caller owns the connection
// (redisFree). Gives up after 5 seconds instead of hanging.
inline redisContext* connectRedis(const Settings& s, std::string& error) {
    struct timeval timeout = {5, 0};
    redisContext* ctx = redisConnectWithTimeout(s.redis_host.c_str(), s.redis_port, timeout);

    if (ctx == nullptr) {
        error = "out of memory";
        return nullptr;
    }
    if (ctx->err) {
        error = std::string(ctx->errstr) + " (" + s.redis_host + ":" + std::to_string(s.redis_port) + ")";
        redisFree(ctx);
        return nullptr;
    }

    if (s.redis_db != 0) {
        redisReply* r = static_cast<redisReply*>(redisCommand(ctx, "SELECT %d", s.redis_db));
        bool ok = (r != nullptr && r->type != REDIS_REPLY_ERROR);
        if (!ok) {
            error = std::string("could not select Redis database ") + std::to_string(s.redis_db) +
                    (r != nullptr && r->str != nullptr ? std::string(": ") + r->str : "");
        }
        if (r != nullptr) {
            freeReplyObject(r);
        }
        if (!ok) {
            redisFree(ctx);
            return nullptr;
        }
    }

    return ctx;
}

}  // namespace config