#ifndef INC_2025_OS_HW2_SINGLE_LINE_BUFFER_H
#define INC_2025_OS_HW2_SINGLE_LINE_BUFFER_H

#include <pthread.h>

typedef struct {
    char *data;
    int full;
    int finished;
    int active_producers;

    pthread_mutex_t lock;
    pthread_cond_t cond_not_full;
    pthread_cond_t cond_not_empty;
} SingleLineBuffer;

void slb_init(SingleLineBuffer *buffer, int n_prod);
void slb_destroy(SingleLineBuffer *buffer);
void slb_put(SingleLineBuffer *buffer, char *item);
char* slb_get(SingleLineBuffer *buffer);
void slb_notify_producer_finished(SingleLineBuffer *buffer);

#endif
