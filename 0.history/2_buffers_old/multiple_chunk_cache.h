#ifndef INC_2025_OS_HW2_SINGLE_LINE_H
#define INC_2025_OS_HW2_SINGLE_LINE_H

#include <stdio.h>
#include <stdlib.h>
#include "../3_processor/char_stat.h"
#include "../4_statistics/thread_metrics.h"

#define CHUNK_SIZE 4096

typedef struct {
    char *data;
    size_t size;
    int chunk_id;
} Chunk;

typedef struct {
    long start_offset;
    long end_offset;
    long current_offset;
} WorkRange;

typedef struct {
    FILE *rfile;
    int fd;
    long file_size;
} FileReader;

typedef struct multiple_chunk_buffer {
    Chunk *chunk;

    pthread_mutex_t lock;
    pthread_cond_t cond_not_full;
    pthread_cond_t cond_not_empty;
    int full;
    int done;
} MultipleChunkBuffer;

typedef struct producer_thread_data {
    int id;
    long handledChunks;
    size_t handledBytes;
    MultipleChunkBuffer *so;
    FileReader *file;
    WorkRange *work_range;
    ThreadMetrics metrics;

    long long cache_refs;
    long long cache_misses;
} ProducerThread;

typedef struct consumer_thread_data {
    int id;
    long handledChunks;
    size_t handledBytes;
    MultipleChunkBuffer *so;
    CharStats *stats;
    ThreadMetrics metrics;

    long long cache_refs;
    long long cache_misses;
} ConsumerThread;

void init_producer_thread(ProducerThread *thread, MultipleChunkBuffer *buffer, FileReader *file, WorkRange *work);
void init_consumer_thread(ConsumerThread *thread, MultipleChunkBuffer *buffer);
void init_file_reader(FileReader *reader, FILE *rfile);
void init_multiple_slot_buffer(MultipleChunkBuffer *buffer);

void *producer(void *arg);
void *consumer(void *arg);

#endif