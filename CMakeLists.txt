cmake_minimum_required(VERSION 3.20)

project(distributed_job_platform)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Drogon CONFIG REQUIRED)
find_package(libpqxx CONFIG REQUIRED)

add_executable(distributed_job_platform-api
    src/api/main.cpp
)

target_include_directories(distributed_job_platform-api
    PRIVATE
        X:/codes/vcpkg/installed/x64-windows/include
)

target_link_libraries(distributed_job_platform-api
    PRIVATE
        Drogon::Drogon
        libpqxx::pqxx
        X:/codes/vcpkg/installed/x64-windows/lib/redis++.lib
        X:/codes/vcpkg/installed/x64-windows/lib/hiredis.lib
)

add_executable(distributed_job_platform-worker
    src/worker/main.cpp
)

target_include_directories(distributed_job_platform-worker
    PRIVATE
        X:/codes/vcpkg/installed/x64-windows/include
)

target_link_libraries(distributed_job_platform-worker
    PRIVATE
        X:/codes/vcpkg/installed/x64-windows/lib/redis++.lib
        X:/codes/vcpkg/installed/x64-windows/lib/hiredis.lib
        libpqxx::pqxx
)