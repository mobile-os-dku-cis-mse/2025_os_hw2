#define _XOPEN_SOURCE 700
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <sys/resource.h>

#include "../components/1_reader/chunk_reader.h"
#include "../components/2_buffer/multiple_chunk_buffer.h"
#include "../components/3_processor/char_stat.h"
#include "../components/4_statistics/timer.h"
#include "../components/common.h"

#define CHUNK_SIZE 4096

typedef struct {
    double io_time_sec;
    double wait_time_sec;
    double cpu_proc_time_sec;
    long chunks_processed;
    long voluntary_switches;
} ThreadMetrics;

typedef struct {
    int id;
    int handledChunks;
    MultiChunkBuffer *so;
    CharStats *stats;
    ThreadMetrics metrics;
} ConsumerThread;

typedef struct {
    int id;
    MultiChunkBuffer *so;
    char *filename;
    ReadRange range;
    ThreadMetrics metrics;
} ProducerThread;

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
        tm->voluntary_switches = usage.ru_nvcsw;
    }
}

void *producer(void *arg) {
    ProducerThread *pt = (ProducerThread *) arg;
    ChunkReader reader;
    struct timespec ts;

    memset(&pt->metrics, 0, sizeof(ThreadMetrics));

    FILE *file = fopen(pt->filename, "r");
    if (!file) {
        perror("fopen");
        exit(1);
    }

    chunk_reader_init(&reader, file, pt->range, CHUNK_SIZE);

    while (1) {
        tm_start(&ts);
        int has_more = chunk_reader_has_more(&reader);
        if (!has_more) break;
        DataUnit unit = chunk_reader_next(&reader);
        pt->metrics.io_time_sec += tm_stop(&ts);

        if (unit.data != NULL) {
            tm_start(&ts);
            mcb_put(pt->so, unit);
            pt->metrics.wait_time_sec += tm_stop(&ts);

            pt->metrics.chunks_processed++;
        } else {
            break;
        }
    }

    tm_update_rusage(&pt->metrics);
    chunk_reader_destroy(&reader);
    fclose(file);
    mcb_notify_producer_finished(pt->so);

    int *ret = malloc(sizeof(int));
    *ret = pt->metrics.chunks_processed;
    return ret;
}

void *consumer(void *arg) {
    ConsumerThread *ct = (ConsumerThread *) arg;
    struct timespec ts;

    memset(&ct->metrics, 0, sizeof(ThreadMetrics));

    while (1) {
        tm_start(&ts);
        DataUnit unit = mcb_get(ct->so);
        ct->metrics.wait_time_sec += tm_stop(&ts);

        if (unit.data == NULL) break;

        tm_start(&ts);
        update_stats_in_chunk((char *) unit.data, ct->stats);
        ct->metrics.cpu_proc_time_sec += tm_stop(&ts);

        free(unit.data);
        ct->metrics.chunks_processed++;
    }

    tm_update_rusage(&ct->metrics);
    ct->handledChunks = ct->metrics.chunks_processed;
    return NULL;
}

void print_detailed_analysis(ProducerThread *prods, int n_prod, ConsumerThread *cons, int n_cons) {
    printf("\n================ [ Detailed Performance Analysis ] ================\n");

    printf("\n[Producer Statistics]\n");
    printf("ID | I/O Time(s) | Put/Wait(s) | CtxSwitch\n");
    printf("---|-------------|-------------|----------\n");

    double total_io = 0, total_prod_wait = 0;

    for (int i = 0; i < n_prod; i++) {
        printf("%2d | %11.4f | %11.4f | %14ld | %15ld | %9ld\n",
               i, prods[i].metrics.io_time_sec, prods[i].metrics.wait_time_sec,
               prods[i].metrics.voluntary_switches);
        total_io += prods[i].metrics.io_time_sec;
        total_prod_wait += prods[i].metrics.wait_time_sec;
    }

    printf("\n[Consumer Statistics]\n");
    printf("ID | Get/Wait(s) | Process(s)  | Chunks  CtxSwitch\n");
    printf("---|-------------|-------------|------------------\n");

    double total_cons_wait = 0, total_proc = 0;

    for (int i = 0; i < n_cons; i++) {
        printf("%2d | %11.4f | %11.4f | %9ld\n",
               i, cons[i].metrics.wait_time_sec, cons[i].metrics.cpu_proc_time_sec,
               cons[i].metrics.voluntary_switches);
        total_cons_wait += cons[i].metrics.wait_time_sec;
        total_proc += cons[i].metrics.cpu_proc_time_sec;
    }

    printf("\n[Bottleneck Diagnosis]\n");
    printf("1. Avg I/O Time     : %.4f sec  (Producer Disk Read)\n", total_io / n_prod);
    printf("2. Avg Locking Time : %.4f sec  (Prod Wait: %.4f, Cons Wait: %.4f)\n",
           (total_prod_wait + total_cons_wait) / (n_prod + n_cons),
           total_prod_wait / n_prod, total_cons_wait / n_cons);
    printf("3. Avg Compute Time : %.4f sec  (Consumer Processing)\n", total_proc / n_cons);

}

int main(int argc, char *argv[]) {
    pthread_t prod[100], cons[100];
    int Nprod, Ncons, BufferSize;
    int *ret_producer;
    MetricsTimer total_timer;

    if (argc == 1) {
        printf("usage: ./detailed_profile <readfile> #Producer #Consumer [BufferSize]\n");
        exit(0);
    }

    Nprod = (argv[2]) ? atoi(argv[2]) : 1;
    if (Nprod > 100) Nprod = 100;
    if (Nprod < 1) Nprod = 1;

    Ncons = (argv[3]) ? atoi(argv[3]) : 1;
    if (Ncons > 100) Ncons = 100;
    if (Ncons < 1) Ncons = 1;

    BufferSize = (argc > 4 && argv[4]) ? atoi(argv[4]) : 10;
    if (BufferSize <= 0) BufferSize = 10;

    MultiChunkBuffer *share = malloc(sizeof(MultiChunkBuffer));
    mcb_init(share, Nprod, BufferSize);

    CharStats stats_main;
    init_stats(&stats_main);

    ConsumerThread *consumers = malloc(sizeof(ConsumerThread) * Ncons);
    ProducerThread *producers = malloc(sizeof(ProducerThread) * Nprod);

    FILE *fp = fopen(argv[1], "r");
    if (!fp) {
        perror("fopen main");
        exit(1);
    }
    fseek(fp, 0, SEEK_END);
    long filesize = ftell(fp);
    fclose(fp);

    long chunk_size_file = filesize / Nprod;

    printf("Starting Profiling... (File: %ld bytes, P:%d, C:%d, Buf:%d)\n", filesize, Nprod, Ncons, BufferSize);
    start_timer(&total_timer);

    for (int i = 0; i < Nprod; i++) {
        producers[i].id = i;
        producers[i].so = share;
        producers[i].filename = argv[1];
        producers[i].range.start = i * chunk_size_file;
        producers[i].range.end = (i == Nprod - 1) ? filesize : (i + 1) * chunk_size_file;
        pthread_create(&prod[i], NULL, producer, &producers[i]);
    }

    for (int i = 0; i < Ncons; i++) {
        consumers[i].so = share;
        consumers[i].stats = malloc(sizeof(CharStats));
        init_stats(consumers[i].stats);
        consumers[i].id = i;
        consumers[i].handledChunks = 0;
        pthread_create(&cons[i], NULL, consumer, &consumers[i]);
    }

    for (int i = 0; i < Ncons; i++) {
        pthread_join(cons[i], NULL);
        accumulate_stats(&stats_main, consumers[i].stats);
        free(consumers[i].stats);
    }

    for (int i = 0; i < Nprod; i++) {
        pthread_join(prod[i], (void **) &ret_producer);
        free(ret_producer);
    }

    stop_timer(&total_timer);

    mcb_destroy(share);
    free(share);

    print_stats(&stats_main);
    print_metrics(&total_timer, Ncons, Nprod);
    print_detailed_analysis(producers, Nprod, consumers, Ncons);

    free(consumers);
    free(producers);

    return 0;
}
