#include "single_line_buffer.h"
#include <stdlib.h>

void slb_init(SingleLineBuffer *buffer, int n_prod) {
    buffer->data = NULL;
    buffer->full = 0;
    buffer->finished = 0;
    buffer->active_producers = n_prod;
    pthread_mutex_init(&buffer->lock, NULL);
    pthread_cond_init(&buffer->cond_not_full, NULL);
    pthread_cond_init(&buffer->cond_not_empty, NULL);
}

void slb_destroy(SingleLineBuffer *buffer) {
    pthread_mutex_destroy(&buffer->lock);
    pthread_cond_destroy(&buffer->cond_not_full);
    pthread_cond_destroy(&buffer->cond_not_empty);
}

void slb_put(SingleLineBuffer *buffer, char *item) {
    pthread_mutex_lock(&buffer->lock);

    while (buffer->full) {
        pthread_cond_wait(&buffer->cond_not_full, &buffer->lock);
    }

    buffer->data = item;
    buffer->full = 1;

    pthread_cond_signal(&buffer->cond_not_empty);
    pthread_mutex_unlock(&buffer->lock);
}

char* slb_get(SingleLineBuffer *buffer) {
    pthread_mutex_lock(&buffer->lock);

    while (!buffer->full && !buffer->finished) {
        pthread_cond_wait(&buffer->cond_not_empty, &buffer->lock);
    }

    if (!buffer->full && buffer->finished) {
        pthread_mutex_unlock(&buffer->lock);
        return NULL;
    }

    char *item = buffer->data;
    buffer->data = NULL;
    buffer->full = 0;

    pthread_cond_signal(&buffer->cond_not_full);
    pthread_mutex_unlock(&buffer->lock);

    return item;
}

void slb_notify_producer_finished(SingleLineBuffer *buffer) {
    pthread_mutex_lock(&buffer->lock);

    buffer->active_producers--;
    if (buffer->active_producers == 0) {
        buffer->finished = 1;
        pthread_cond_broadcast(&buffer->cond_not_empty);
    }

    pthread_mutex_unlock(&buffer->lock);
}