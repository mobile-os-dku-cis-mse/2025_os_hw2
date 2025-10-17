#ifndef INC_2025_OS_HW2_PERFORMANCE_H
#define INC_2025_OS_HW2_PERFORMANCE_H
#include <sys/resource.h>
#include <time.h>

typedef struct {
    struct timespec start_wall;
    struct timespec end_wall;
    struct rusage start_cpu;
    struct rusage end_cpu;
} MetricsTimer;

void start_timer(MetricsTimer *timer);
void stop_timer(MetricsTimer *timer);
void print_metrics(MetricsTimer *timer, int Ncon, int NPro);

#endif