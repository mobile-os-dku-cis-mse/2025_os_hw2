#include "multiple_chunk_buffer.h"
#include <stdlib.h>
#include <stdio.h>

void mcb_init(MultiChunkBuffer *buffer, int n_prod, int capacity) {
    buffer->data_array = malloc(sizeof(DataUnit) * capacity);
    buffer->capacity = capacity;
    
    buffer->head = 0;
    buffer->tail = 0;
    buffer->count = 0;
    
    buffer->finished = 0;
    buffer->active_producers = n_prod;
    
    pthread_mutex_init(&buffer->lock, NULL);
    pthread_cond_init(&buffer->cond_not_full, NULL);
    pthread_cond_init(&buffer->cond_not_empty, NULL);
}

void mcb_destroy(MultiChunkBuffer *buffer) {
    free(buffer->data_array);
    pthread_mutex_destroy(&buffer->lock);
    pthread_cond_destroy(&buffer->cond_not_full);
    pthread_cond_destroy(&buffer->cond_not_empty);
}

void mcb_put(MultiChunkBuffer *buffer, DataUnit unit) {
    pthread_mutex_lock(&buffer->lock);

    while (buffer->count == buffer->capacity) {
        pthread_cond_wait(&buffer->cond_not_full, &buffer->lock);
    }

    buffer->data_array[buffer->tail] = unit;
    buffer->tail = (buffer->tail + 1) % buffer->capacity;
    buffer->count++;

    pthread_cond_signal(&buffer->cond_not_empty);
    pthread_mutex_unlock(&buffer->lock);
}

DataUnit mcb_get(MultiChunkBuffer *buffer) {
    DataUnit empty_unit = { .data = NULL, .size = 0, .id = -1 };

    pthread_mutex_lock(&buffer->lock);

    while (buffer->count == 0 && !buffer->finished) {
        pthread_cond_wait(&buffer->cond_not_empty, &buffer->lock);
    }

    if (buffer->count == 0 && buffer->finished) {
        pthread_mutex_unlock(&buffer->lock);
        return empty_unit;
    }

    DataUnit item = buffer->data_array[buffer->head];
    buffer->head = (buffer->head + 1) % buffer->capacity;
    buffer->count--;

    pthread_cond_signal(&buffer->cond_not_full);
    pthread_mutex_unlock(&buffer->lock);

    return item;
}

void mcb_notify_producer_finished(MultiChunkBuffer *buffer) {
    pthread_mutex_lock(&buffer->lock);
    buffer->active_producers--;
    if (buffer->active_producers == 0) {
        buffer->finished = 1;
        pthread_cond_broadcast(&buffer->cond_not_empty);
    }
    pthread_mutex_unlock(&buffer->lock);
}

DataUnit mcb_try_get(MultiChunkBuffer *buffer) {
    DataUnit fail_unit = { .data = NULL, .size = 0, .id = -2 };

    if (pthread_mutex_trylock(&buffer->lock) != 0) {
        return fail_unit;
    }

    if (buffer->count == 0) {
        pthread_mutex_unlock(&buffer->lock);
        return fail_unit;
    }

    DataUnit item = buffer->data_array[buffer->head];
    buffer->head = (buffer->head + 1) % buffer->capacity;
    buffer->count--;

    pthread_cond_signal(&buffer->cond_not_full);
    pthread_mutex_unlock(&buffer->lock);

    return item;
}