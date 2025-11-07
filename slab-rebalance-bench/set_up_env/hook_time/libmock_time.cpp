#define _GNU_SOURCE
#include <iostream>
#include <chrono>
#include <ctime>
#include <dlfcn.h>
#include <time.h>
#include <atomic>

// Define function pointer type for original clock_gettime and time
typedef int (*clock_gettime_t)(clockid_t, struct timespec*);
typedef time_t (*time_t_func)(time_t*);

static clock_gettime_t real_clock_gettime = nullptr;
static time_t_func real_time = nullptr;
static std::atomic<time_t> mock_time_sec{0};

// Function to set mock time (Exposed via shared library)
extern "C" void set_mock_time(time_t sec) {
    mock_time_sec.store(sec, std::memory_order_relaxed);
}

// Hooked clock_gettime function
extern "C" int clock_gettime(clockid_t clk_id, struct timespec* tp) {
    if (!real_clock_gettime) {
        real_clock_gettime = (clock_gettime_t)dlsym(RTLD_NEXT, "clock_gettime");
    }

    tp->tv_sec = mock_time_sec.load(std::memory_order_relaxed);
    tp->tv_nsec = 0;
    return 0;
}

// Hooked time function
extern "C" time_t time(time_t* t) {
    if (!real_time) {
        real_time = (time_t_func)dlsym(RTLD_NEXT, "time");
    }

    time_t sec = mock_time_sec.load(std::memory_order_relaxed);
    if (t) {
        *t = sec;
    }
    return sec;
}

// Initialization
__attribute__((constructor)) void init() {
    real_clock_gettime = (clock_gettime_t)dlsym(RTLD_NEXT, "clock_gettime");
    real_time = (time_t_func)dlsym(RTLD_NEXT, "time");
}