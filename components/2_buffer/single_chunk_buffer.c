#include "single_chunk_buffer.h"
#include <stdlib.h>

void scb_init(SingleChunkBuffer *buffer, int n_prod) {
    buffer->unit.data = NULL;
    buffer->unit.size = 0;
    buffer->unit.id = -1;

    buffer->full = 0;
    buffer->finished = 0;
    buffer->active_producers = n_prod;

    pthread_mutex_init(&buffer->lock, NULL);
    pthread_cond_init(&buffer->cond_not_full, NULL);
    pthread_cond_init(&buffer->cond_not_empty, NULL);
}

void scb_destroy(SingleChunkBuffer *buffer) {
    pthread_mutex_destroy(&buffer->lock);
    pthread_cond_destroy(&buffer->cond_not_full);
    pthread_cond_destroy(&buffer->cond_not_empty);
}

void scb_put(SingleChunkBuffer *buffer, DataUnit unit) {
    pthread_mutex_lock(&buffer->lock);

    while (buffer->full) {
        pthread_cond_wait(&buffer->cond_not_full, &buffer->lock);
    }

    buffer->unit = unit;
    buffer->full = 1;

    pthread_cond_signal(&buffer->cond_not_empty);
    pthread_mutex_unlock(&buffer->lock);
}

DataUnit scb_get(SingleChunkBuffer *buffer) {
    DataUnit empty_unit = { .data = NULL, .size = 0, .id = -1 };

    pthread_mutex_lock(&buffer->lock);

    while (!buffer->full && !buffer->finished) {
        pthread_cond_wait(&buffer->cond_not_empty, &buffer->lock);
    }

    if (!buffer->full && buffer->finished) {
        pthread_mutex_unlock(&buffer->lock);
        return empty_unit;
    }

    DataUnit item = buffer->unit;
    buffer->unit = empty_unit;
    buffer->full = 0;

    pthread_cond_signal(&buffer->cond_not_full);
    pthread_mutex_unlock(&buffer->lock);

    return item;
}

void scb_notify_producer_finished(SingleChunkBuffer *buffer) {
    pthread_mutex_lock(&buffer->lock);
    buffer->active_producers--;
    if (buffer->active_producers == 0) {
        buffer->finished = 1;
        pthread_cond_broadcast(&buffer->cond_not_empty);
    }
    pthread_mutex_unlock(&buffer->lock);
}