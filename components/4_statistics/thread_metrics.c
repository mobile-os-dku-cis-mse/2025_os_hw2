#include "thread_metrics.h"
#include <string.h>
#include <stdio.h>
#include <unistd.h>

void tm_init(ThreadMetrics *tm) {
    memset(tm, 0, sizeof(ThreadMetrics));
}

void tm_start(struct timespec *ts) {
    clock_gettime(CLOCK_MONOTONIC, ts);
}

double tm_stop(struct timespec *ts) {
    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);
    return (end.tv_sec - ts->tv_sec) + (end.tv_nsec - ts->tv_nsec) / 1e9;
}

void tm_update_rusage(ThreadMetrics *tm) {
    struct rusage usage;
    if (getrusage(RUSAGE_THREAD, &usage) == 0) {
        tm->min_page_faults = usage.ru_minflt;
        tm->maj_page_faults = usage.ru_majflt;
        tm->voluntary_switches = usage.ru_nvcsw;
    }
}