#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <time.h>
#include <sys/resource.h>
#include "timer.h"

void start_timer(MetricsTimer *timer) {
    if (clock_gettime(CLOCK_MONOTONIC, &timer->start_wall) != 0) {
        perror("clock_gettime start_wall");
        exit(1);
    }
    if (getrusage(RUSAGE_SELF, &timer->start_cpu) != 0) {
        perror("getrusage start_cpu");
        exit(1);
    }
}

void stop_timer(MetricsTimer *timer) {
    if (clock_gettime(CLOCK_MONOTONIC, &timer->end_wall) != 0) {
        perror("clock_gettime end_wall");
        exit(1);
    }
    if (getrusage(RUSAGE_SELF, &timer->end_cpu) != 0) {
        perror("getrusage end_cpu");
        exit(1);
    }
}

static double get_timeval_diff(struct timeval *start, struct timeval *end) {
    return (end->tv_sec - start->tv_sec) + (end->tv_usec - start->tv_usec) / 1e6;
}

static double get_timespec_diff(struct timespec *start, struct timespec *end) {
    return (end->tv_sec - start->tv_sec) + (end->tv_nsec - start->tv_nsec) / 1e9;
}

void print_metrics(MetricsTimer *timer, int Ncons, int Nprod) {
    double real_sec = get_timespec_diff(&timer->start_wall, &timer->end_wall);
    double user_sec = get_timeval_diff(&timer->start_cpu.ru_utime, &timer->end_cpu.ru_utime);
    double sys_sec = get_timeval_diff(&timer->start_cpu.ru_stime, &timer->end_cpu.ru_stime);

    double cpu_total_sec = user_sec + sys_sec;
    double total_cpu_percent = (cpu_total_sec / real_sec) * 100.0;

    printf("\n---[ Performance ]---\n");
    printf("#Consumer: %d\n#Producer: %d\n", Ncons, Nprod);
    printf("1. Total Time (real) : %.3f sec\n", real_sec);
    printf("2. CPU Time (user)   : %.3f sec\n", user_sec);
    printf("3. CPU Time (sys)    : %.3f sec\n", sys_sec);
    printf("4. CPU Usage    : %.1f %% \n", total_cpu_percent);
    printf("---------------------------------\n");
}
