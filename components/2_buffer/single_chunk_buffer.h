#ifndef INC_2025_OS_HW2_SINGLE_CHUNK_BUFFER_H
#define INC_2025_OS_HW2_SINGLE_CHUNK_BUFFER_H

#include <pthread.h>
#include "../common.h"

typedef struct {
    DataUnit unit;
    int full;
    int finished;
    int active_producers;

    pthread_mutex_t lock;
    pthread_cond_t cond_not_full;
    pthread_cond_t cond_not_empty;
} SingleChunkBuffer;

void scb_init(SingleChunkBuffer *buffer, int n_prod);
void scb_destroy(SingleChunkBuffer *buffer);
void scb_put(SingleChunkBuffer *buffer, DataUnit unit);
DataUnit scb_get(SingleChunkBuffer *buffer);
void scb_notify_producer_finished(SingleChunkBuffer *buffer);

#endif