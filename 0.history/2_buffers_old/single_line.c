#define _XOPEN_SOURCE 700

#include "single_line.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "../3_processor/char_stat.h"

void init_consumer_thread(ConsumerThread* thread, SingleLineBuffer* buffer){
    memset(thread, 0, sizeof(ConsumerThread));
    thread ->handledChunks = 0;
    thread ->so = buffer;
    thread->stats = malloc(sizeof(CharStats));

    init_stats(thread ->stats);
}

void *consumer(void *arg) {
    ConsumerThread *thread = (ConsumerThread *) arg;
    SingleLineBuffer *buffer = thread->so;

    int i = 0;
    char *line;
    int linenum;

    while (1) {
        pthread_mutex_lock(&buffer->lock);

        while (buffer->full == 0 && !buffer->eof) {
            pthread_cond_wait(&buffer->cond_not_empty, &buffer->lock);
        }

        if (buffer->eof && !buffer->full) {
            pthread_cond_broadcast(&buffer->cond_not_empty);
            pthread_cond_broadcast(&buffer->cond_not_full);
            pthread_mutex_unlock(&buffer->lock);
            break;
        }

        line = buffer->line;
        linenum = buffer->linenum;
        buffer->line = NULL;
        buffer->full = 0;

        pthread_cond_signal(&buffer->cond_not_full);
        pthread_mutex_unlock(&buffer->lock);

        printf("Cons_%x: [%02d:%02d] %s",
               (unsigned int) pthread_self(), i, linenum, line);
        update_stats_in_line(line, thread->stats);
        free(line);
        thread->handledChunks++;
    }
    printf("Cons: %d lines\n", i);

    pthread_exit(thread);
}

void *producer(void *arg) {
    SingleLineBuffer *so = arg;
    int *ret = malloc(sizeof(int));
    FILE *rfile = so->rfile;
    int i = 0;
    char *line = NULL;
    size_t len = 0;
    ssize_t read = 0;

    while (1) {
        pthread_mutex_lock(&so->lock);

        while (so->full && !so->eof) {
            pthread_cond_wait(&so->cond_not_full, &so->lock);
        }

        if (so->eof) {
            pthread_mutex_unlock(&so->lock);
            break;
        }

        read = getdelim(&line, &len, '\n', rfile);

        if (read == -1) {
            so->eof = 1;
            so->full = 0;

            pthread_cond_broadcast(&so->cond_not_empty);
            pthread_cond_broadcast(&so->cond_not_full);
            pthread_mutex_unlock(&so->lock);
            break;
        }

        so->linenum = i;
        so->line = strdup(line);
        i++;
        so->full = 1;

        pthread_cond_signal(&so->cond_not_empty);
        pthread_mutex_unlock(&so->lock);
    }

    free(line);
    printf("Prod_%x: %d lines\n", (unsigned int) pthread_self(), i);
    *ret = i;
    pthread_exit(ret);
}
