#ifndef INC_2025_OS_HW2_MULTIPLE_CHUNK_BUFFER_H
#define INC_2025_OS_HW2_MULTIPLE_CHUNK_BUFFER_H

#include <pthread.h>
#include "../common.h"

typedef struct {
    DataUnit *data_array;
    int capacity;

    int head;
    int tail;
    int count;

    int finished;
    int active_producers;

    pthread_mutex_t lock;
    pthread_cond_t cond_not_full;
    pthread_cond_t cond_not_empty;
} MultiChunkBuffer;

void mcb_init(MultiChunkBuffer *buffer, int n_prod, int capacity);
void mcb_destroy(MultiChunkBuffer *buffer);
void mcb_put(MultiChunkBuffer *buffer, DataUnit unit);
DataUnit mcb_get(MultiChunkBuffer *buffer);
void mcb_notify_producer_finished(MultiChunkBuffer *buffer);

DataUnit mcb_try_get(MultiChunkBuffer *buffer);

#endif //INC_2025_OS_HW2_MULTIPLE_CHUNK_BUFFER_H