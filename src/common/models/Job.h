#pragma once

#include <string>
using namespace std;

enum class JobStatus {
    QUEUED,
    RUNNING,
    COMPLETED,
    FAILED
};

enum class JobPriority {
    LOW = 0,
    NORMAL = 1,
    HIGH = 2
};

struct Job {
    string id;
    string type;
    string payload;

    JobStatus status;

    JobPriority priority;
};