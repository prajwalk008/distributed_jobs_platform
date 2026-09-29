# Build from the repo root:  docker build -f docker/worker.Dockerfile -t taskflow-worker .
#
# Same two-stage shape as api.Dockerfile. The worker never uses Drogon, so its
# runtime image skips the whole HTTP-framework dependency chain (no SQLite,
# MariaDB client, yaml-cpp, brotli, ...) and stays considerably smaller.
#
# Pinned to 24.04 specifically: every package name below was confirmed against
# a real Ubuntu 24.04 apt-get run, not guessed.

# ---------------------------------------------------------------- build ----
FROM ubuntu:24.04 AS build

ENV DEBIAN_FRONTEND=noninteractive

# The worker's own CMake target doesn't link Drogon, but libdrogon-dev is
# still installed here because src/config/Config.h is shared with the API and
# the two are compiled together by the same CMakeLists.txt; only Drogon's
# headers are actually touched, none of its libraries end up in the worker
# binary (confirmed via ldd on the built binary further down).
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
        pkg-config \
        libdrogon-dev \
        libpqxx-dev \
        libhiredis-dev \
        libjsoncpp-dev \
        libssl-dev \
        uuid-dev \
        zlib1g-dev \
        libc-ares-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY CMakeLists.txt .
COPY src ./src

RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build --target distributed_job_platform-worker -j"$(nproc)"

# -------------------------------------------------------------- runtime ----
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# libpqxx-7.8t64 and libhiredis1.1.0 each declare their own further
# dependencies (SSL, the PostgreSQL client library, Kerberos/LDAP for network
# auth, ...) in their apt metadata, so apt resolves that chain on its own.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpqxx-7.8t64 \
        libhiredis1.1.0 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --shell /usr/sbin/nologin taskflow
USER taskflow
WORKDIR /home/taskflow

COPY --from=build /src/build/distributed_job_platform-worker /usr/local/bin/taskflow-worker

# Exec form (a JSON array, not a bare string) matters here: it makes this
# binary PID 1 in the container, so `docker stop`'s SIGTERM goes straight to
# it. The shell form would run it under /bin/sh -c '...', and sh does not
# forward SIGTERM to its child — which would silently defeat the graceful-
# shutdown handling already built into the worker (stop taking new jobs,
# finish the one in hand, exit 0).
CMD ["/usr/local/bin/taskflow-worker"]
