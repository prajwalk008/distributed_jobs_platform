# Build from the repo root:  docker build -f docker/api.Dockerfile -t taskflow-api .
#
# Two stages: "build" has the full compiler toolchain and every -dev package;
# the final image has only what the binary actually needs at runtime, which
# keeps it far smaller and avoids shipping a compiler in production.
#
# Pinned to 24.04 specifically: every package name below was confirmed against
# a real Ubuntu 24.04 apt-get run, not guessed. A different Ubuntu/Debian
# version can use different package names for the same libraries.

# ---------------------------------------------------------------- build ----
FROM ubuntu:24.04 AS build

ENV DEBIAN_FRONTEND=noninteractive

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
    && cmake --build build --target distributed_job_platform-api -j"$(nproc)"

# -------------------------------------------------------------- runtime ----
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Each of these three packages declares its own further dependencies (SSL,
# PostgreSQL client, SQLite, MariaDB client, yaml-cpp, brotli, Kerberos/LDAP
# for network auth, ...) in its apt metadata, so apt resolves that entire
# chain on its own — nothing beyond these three needs to be named here.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libdrogon1t64 \
        libpqxx-7.8t64 \
        libhiredis1.1.0 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --shell /usr/sbin/nologin taskflow
USER taskflow
WORKDIR /home/taskflow

COPY --from=build /src/build/distributed_job_platform-api /usr/local/bin/taskflow-api

EXPOSE 8080

# Exec form (a JSON array, not a bare string) matters here specifically: it
# makes this binary PID 1 in the container, so `docker stop`'s SIGTERM goes
# straight to it. The shell form would run it under /bin/sh -c '...' instead,
# and sh does not forward SIGTERM to its child — which would silently defeat
# the graceful-shutdown handling already built into the worker (and, if this
# binary ever needs it too, the API).
CMD ["/usr/local/bin/taskflow-api"]
