#include <word_count.h>

void queue_init(queue_t *q, int cap) {
    q->buf = (char**)calloc(cap, sizeof(char*));
    if (!q->buf) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }
    q->cap = cap;
    q->head = q->tail = q->count = 0;
    q->done = false;
    pthread_mutex_init(&q->m, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
}

void queue_destroy(queue_t *q) {
    free(q->buf);
    pthread_mutex_destroy(&q->m);
    pthread_cond_destroy(&q->not_empty);
    pthread_cond_destroy(&q->not_full);
}

void queue_push(queue_t *q, char *line) {
    pthread_mutex_lock(&q->m);
    while (q->count == q->cap) {
        pthread_cond_wait(&q->not_full, &q->m);
    }
    q->buf[q->tail] = line;
    q->tail = (q->tail + 1) % q->cap;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->m);
}

int queue_pop(queue_t *q, char **out_line) {
    pthread_mutex_lock(&q->m);
    while (q->count == false && !q->done) {
        pthread_cond_wait(&q->not_empty, &q->m);
    }
    if (q->count == false && q->done) {
        pthread_mutex_unlock(&q->m);
        return 0;
    }
    char *line = q->buf[q->head];
    q->head = (q->head + 1) % q->cap;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->m);
    *out_line = line;
    return 1;
}