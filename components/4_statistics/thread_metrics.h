#ifndef THREAD_METRICS_H
#define THREAD_METRICS_H

#define _GNU_SOURCE
#include <time.h>
#include <sys/resource.h>

typedef struct {
    double io_time_sec;
    double wait_time_sec;
    double cpu_proc_time_sec;

    long chunks_processed;
    long bytes_processed;

    long min_page_faults;
    long maj_page_faults;
    long voluntary_switches;
} ThreadMetrics;

void tm_init(ThreadMetrics *tm);
void tm_start(struct timespec *ts);
double tm_stop(struct timespec *ts);
void tm_update_rusage(ThreadMetrics *tm);

#endif