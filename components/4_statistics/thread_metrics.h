#ifndef INC_2025_OS_HW2_THREAD_METRICS_H
#define INC_2025_OS_HW2_THREAD_METRICS_H

#include <time.h>
#include <pthread.h>

typedef struct {
    struct timespec start_time;
    struct timespec end_time;
    double total_waiting_time;
    double total_execution_time;
    double total_time;
    long operation_count;
    pthread_mutex_t lock;
} ThreadMetrics;

void init_thread_metrics(ThreadMetrics *metrics);

void start_thread_timer(ThreadMetrics *metrics);
void stop_thread_timer(ThreadMetrics *metrics);

void start_waiting(struct timespec *wait_start);
void stop_waiting(ThreadMetrics *metrics, struct timespec *wait_start);

void start_execution(struct timespec *exec_start);
void stop_execution(ThreadMetrics *metrics, struct timespec *exec_start);

void finalize_metrics(ThreadMetrics *metrics);

void destroy_thread_metrics(ThreadMetrics *metrics);

double timespec_diff(struct timespec *start, struct timespec *end);

typedef struct {
    double total_waiting_time;
    double total_execution_time;
    double total_time;
    double avg_waiting_time;
    double avg_execution_time;
    double max_waiting_time;
    double max_execution_time;
    double min_waiting_time;
    double min_execution_time;
    long total_operations;
    int thread_count;
} AggregatedMetrics;

void init_aggregated_metrics(AggregatedMetrics *agg);

void add_thread_metrics(AggregatedMetrics *agg, ThreadMetrics *metrics);

void calculate_averages(AggregatedMetrics *agg);

void print_aggregated_metrics(const char *thread_type, AggregatedMetrics *agg);



#endif