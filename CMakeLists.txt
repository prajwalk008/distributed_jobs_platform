cmake_minimum_required(VERSION 3.20)

project(distributed_job_platform)

if (WIN32)
    set(CMAKE_CXX_STANDARD 20)
else()
    # Ubuntu's libpqxx.so is built without std::source_location support
    # (a C++20 feature); libpqxx's headers conditionally add a
    # std::source_location parameter to several exception constructors when
    # compiled at C++20, which then don't match what the shipped .so
    # actually exports (an "undefined reference" at link time, not a
    # compile error). Nothing in this project needs C++20, so building at
    # C++17 here avoids the mismatch entirely.
    set(CMAKE_CXX_STANDARD 17)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

if (WIN32)
    # --- Windows / vcpkg (unchanged from before) ---
    # If your vcpkg lives somewhere other than X:/codes/vcpkg, change this
    # one line; nothing else below needs to know.
    set(VCPKG_INSTALLED "X:/codes/vcpkg/installed/x64-windows" CACHE PATH
        "vcpkg install root")

    find_package(Drogon CONFIG REQUIRED)
    find_package(libpqxx CONFIG REQUIRED)

    set(TASKFLOW_DROGON_LIBS Drogon::Drogon)
    set(TASKFLOW_PQXX_TARGET libpqxx::pqxx)

    # This vcpkg port doesn't give hiredis a usable CMake config, so it's
    # linked directly as a .lib, same as it always has been here.
    set(TASKFLOW_INCLUDE_DIRS "${VCPKG_INSTALLED}/include")
    set(TASKFLOW_REDIS_LIBS
        "${VCPKG_INSTALLED}/lib/redis++.lib"
        "${VCPKG_INSTALLED}/lib/hiredis.lib"
    )
else()
    # --- Linux (apt packages, and inside Docker) ---
    #
    # Two things Ubuntu's apt packages do differently from vcpkg, both worked
    # around below rather than fought:
    #
    # 1. libpqxx-dev and libhiredis-dev ship only .pc files, no CMake config,
    #    so both are found via pkg-config instead of find_package(... CONFIG).
    #
    # 2. libdrogon-dev's CMake config (DrogonConfig.cmake) transitively
    #    requires MySQL AND SQLite3 support to resolve, even though this
    #    project only uses PostgreSQL — and its bundled FindMySQL.cmake looks
    #    for library names (mysqlclient_r, mariadbclient) that don't exist on
    #    modern Ubuntu, so that path fails outright. Going through
    #    find_package(Drogon CONFIG) therefore doesn't work here at all,
    #    Debian packaging bug, not a project misconfiguration. Drogon is
    #    linked directly instead, the same libraries the API has always
    #    linked against when built by hand on this machine.
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(PQXX REQUIRED IMPORTED_TARGET libpqxx)
    pkg_check_modules(HIREDIS REQUIRED IMPORTED_TARGET hiredis)
    # Ubuntu installs jsoncpp's headers under .../jsoncpp/json/json.h; Drogon's
    # own headers #include <json/json.h> and expect that directory on the
    # include path directly, which only pkg-config knows how to locate.
    pkg_check_modules(JSONCPP REQUIRED IMPORTED_TARGET jsoncpp)

    set(TASKFLOW_DROGON_LIBS drogon trantor jsoncpp ssl crypto uuid z cares)
    set(TASKFLOW_PQXX_TARGET PkgConfig::PQXX)
    set(TASKFLOW_INCLUDE_DIRS ${JSONCPP_INCLUDE_DIRS})
    set(TASKFLOW_REDIS_LIBS PkgConfig::HIREDIS)
endif()

add_executable(distributed_job_platform-api
    src/api/main.cpp
)

target_include_directories(distributed_job_platform-api
    PRIVATE
        ${TASKFLOW_INCLUDE_DIRS}
)

target_link_libraries(distributed_job_platform-api
    PRIVATE
        ${TASKFLOW_DROGON_LIBS}
        ${TASKFLOW_PQXX_TARGET}
        ${TASKFLOW_REDIS_LIBS}
)

add_executable(distributed_job_platform-worker
    src/worker/main.cpp
)

target_include_directories(distributed_job_platform-worker
    PRIVATE
        ${TASKFLOW_INCLUDE_DIRS}
)

target_link_libraries(distributed_job_platform-worker
    PRIVATE
        ${TASKFLOW_PQXX_TARGET}
        ${TASKFLOW_REDIS_LIBS}
        Threads::Threads
)