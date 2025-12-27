#ifndef INC_2025_OS_HW2_SINGLE_LINE_H
#define INC_2025_OS_HW2_SINGLE_LINE_H

#include <stdio.h>
#include <stdlib.h>
#include "../char_stat.h"

typedef struct single_line_buffer {
    FILE *rfile;
    int linenum;
    char *line;
    pthread_mutex_t lock;
    pthread_cond_t cond_not_full;
    pthread_cond_t cond_not_empty;
    int full;
    int eof;
} SingleLineBuffer;

typedef struct consumer_thread_data {
    int id;
    int handledChunks;
    SingleLineBuffer *so;
    CharStats * stats;
} ConsumerThread;

typedef struct producer_thread_data {
    int idx;
    SingleLineBuffer *so;
    CharStats * stats;
} ProducerThread;

void *consumer(void *arg);
void *producer(void *arg);
void init_consumer_thread(ConsumerThread* thread, SingleLineBuffer* buffer);

#endif //INC_2025_OS_HW2_SINGLE_LINE_H