#include "thread_metrics.h"
#include <stdio.h>
#include <stdlib.h>
#include <float.h>

double timespec_diff(struct timespec *start, struct timespec *end) {
    return (end->tv_sec - start->tv_sec) + 
           (end->tv_nsec - start->tv_nsec) / 1e9;
}

void init_thread_metrics(ThreadMetrics *metrics) {
    metrics->total_waiting_time = 0.0;
    metrics->total_execution_time = 0.0;
    metrics->total_time = 0.0;
    metrics->operation_count = 0;
    pthread_mutex_init(&metrics->lock, NULL);
}

void start_thread_timer(ThreadMetrics *metrics) {
    clock_gettime(CLOCK_MONOTONIC, &metrics->start_time);
}

void stop_thread_timer(ThreadMetrics *metrics) {
    clock_gettime(CLOCK_MONOTONIC, &metrics->end_time);
}

void start_waiting(struct timespec *wait_start) {
    clock_gettime(CLOCK_MONOTONIC, wait_start);
}

void stop_waiting(ThreadMetrics *metrics, struct timespec *wait_start) {
    struct timespec wait_end;
    clock_gettime(CLOCK_MONOTONIC, &wait_end);
    
    double elapsed = timespec_diff(wait_start, &wait_end);
    
    pthread_mutex_lock(&metrics->lock);
    metrics->total_waiting_time += elapsed;
    pthread_mutex_unlock(&metrics->lock);
}

void start_execution(struct timespec *exec_start) {
    clock_gettime(CLOCK_MONOTONIC, exec_start);
}

void stop_execution(ThreadMetrics *metrics, struct timespec *exec_start) {
    struct timespec exec_end;
    clock_gettime(CLOCK_MONOTONIC, &exec_end);
    
    double elapsed = timespec_diff(exec_start, &exec_end);
    
    pthread_mutex_lock(&metrics->lock);
    metrics->total_execution_time += elapsed;
    metrics->operation_count++;
    pthread_mutex_unlock(&metrics->lock);
}

void finalize_metrics(ThreadMetrics *metrics) {
    metrics->total_time = timespec_diff(&metrics->start_time, &metrics->end_time);
}

void destroy_thread_metrics(ThreadMetrics *metrics) {
    pthread_mutex_destroy(&metrics->lock);
}

void init_aggregated_metrics(AggregatedMetrics *agg) {
    agg->total_waiting_time = 0.0;
    agg->total_execution_time = 0.0;
    agg->total_time = 0.0;
    agg->avg_waiting_time = 0.0;
    agg->avg_execution_time = 0.0;
    agg->max_waiting_time = 0.0;
    agg->max_execution_time = 0.0;
    agg->min_waiting_time = DBL_MAX;
    agg->min_execution_time = DBL_MAX;
    agg->total_operations = 0;
    agg->thread_count = 0;
}

void add_thread_metrics(AggregatedMetrics *agg, ThreadMetrics *metrics) {
    agg->total_waiting_time += metrics->total_waiting_time;
    agg->total_execution_time += metrics->total_execution_time;
    agg->total_time += metrics->total_time;
    agg->total_operations += metrics->operation_count;

    if (metrics->total_waiting_time > agg->max_waiting_time) {
        agg->max_waiting_time = metrics->total_waiting_time;
    }
    if (metrics->total_waiting_time < agg->min_waiting_time) {
        agg->min_waiting_time = metrics->total_waiting_time;
    }

    if (metrics->total_execution_time > agg->max_execution_time) {
        agg->max_execution_time = metrics->total_execution_time;
    }
    if (metrics->total_execution_time < agg->min_execution_time) {
        agg->min_execution_time = metrics->total_execution_time;
    }

    agg->thread_count++;
}

void calculate_averages(AggregatedMetrics *agg) {
    if (agg->thread_count > 0) {
        agg->avg_waiting_time = agg->total_waiting_time / agg->thread_count;
        agg->avg_execution_time = agg->total_execution_time / agg->thread_count;
    }

    if (agg->thread_count == 0) {
        agg->min_waiting_time = 0.0;
        agg->min_execution_time = 0.0;
    }
}

void print_aggregated_metrics(const char *thread_type, AggregatedMetrics *agg) {
    printf("\n========== %s Metrics ==========\n", thread_type);
    printf("Number of threads: %d\n", agg->thread_count);
    printf("Total operations: %ld\n\n", agg->total_operations);

    printf("--- Waiting Time (Mutex) ---\n");
    printf("  Total   : %.6f sec\n", agg->total_waiting_time);
    printf("  Average : %.6f sec\n", agg->avg_waiting_time);
    printf("  Max     : %.6f sec\n", agg->max_waiting_time);
    printf("  Min     : %.6f sec\n", agg->min_waiting_time);

    printf("\n--- Execution Time (Work) ---\n");
    printf("  Total   : %.6f sec\n", agg->total_execution_time);
    printf("  Average : %.6f sec\n", agg->avg_execution_time);
    printf("  Max     : %.6f sec\n", agg->max_execution_time);
    printf("  Min     : %.6f sec\n", agg->min_execution_time);

    printf("\n--- Thread Lifetime ---\n");
    printf("  Total   : %.6f sec\n", agg->total_time);
    printf("  Average : %.6f sec\n", agg->total_time / agg->thread_count);

    if (agg->total_time > 0) {
        double wait_percent = (agg->total_waiting_time / agg->total_time) * 100.0;
        double exec_percent = (agg->total_execution_time / agg->total_time) * 100.0;
        printf("\n--- Time Distribution ---\n");
        printf("  Waiting   : %.2f%%\n", wait_percent);
        printf("  Execution : %.2f%%\n", exec_percent);
        printf("  Other     : %.2f%%\n", 100.0 - wait_percent - exec_percent);
    }
    printf("========================================\n");
}