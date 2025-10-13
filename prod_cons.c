#define _XOPEN_SOURCE 700

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "char_stat.h"

typedef struct sharedobject {
    FILE *rfile;
    int linenum;
    char *line;
    pthread_mutex_t lock;
    pthread_cond_t cond_not_full;
    pthread_cond_t cond_not_empty;
    int full;
    int eof;
} so_t;

typedef struct ret_from_thread {
    int i;
    CharStats *stats;
} RetFromThread;

void *producer(void *arg) {
    so_t *so = arg;
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

void *consumer(void *arg) {
    so_t *so = arg;
    RetFromThread *ret = malloc(sizeof(RetFromThread));
    int i = 0;
    int len;
    char *line;

    CharStats* stats = malloc(sizeof(CharStats));
    init_stats(stats);

    while (1) {
        pthread_mutex_lock(&so->lock);

        while (so->full == 0 && !so->eof) {
            pthread_cond_wait(&so->cond_not_empty, &so->lock);
        }

        if (so->eof && !so->full) {
            pthread_cond_broadcast(&so->cond_not_empty);
            pthread_cond_broadcast(&so->cond_not_full);
            pthread_mutex_unlock(&so->lock);
            break;
        }

        line = so->line;
        so->line = NULL;
        so->full = 0;

        pthread_cond_signal(&so->cond_not_full);
        pthread_mutex_unlock(&so->lock);

        len = strlen(line);
        printf("Cons_%x: [%02d:%02d] %s",
               (unsigned int) pthread_self(), i, so->linenum, line);
        update_stats_in_line(line, stats);
        free(line);
        i++;
    }
    printf("Cons: %d lines\n", i);

    ret->i = i;
    ret->stats = stats;

    pthread_exit(ret);
}

int main(int argc, char *argv[]) {
    pthread_t prod[100];
    pthread_t cons[100];
    int Nprod, Ncons;
    int rc;
    long t;
    RetFromThread *ret;
    int* ret_producer;
    int i;
    FILE *rfile;
    if (argc == 1) {
        printf("usage: ./prod_cons <readfile> #Producer #Consumer\n");
        exit(0);
    }
    so_t *share = malloc(sizeof(so_t));
    memset(share, 0, sizeof(so_t));
    rfile = fopen((char *) argv[1], "r");
    if (rfile == NULL) {
        perror("rfile");
        exit(0);
    }
    if (argv[2] != NULL) {
        Nprod = atoi(argv[2]);
        if (Nprod > 100) Nprod = 100;
        if (Nprod == 0) Nprod = 1;
    } else Nprod = 1;
    if (argv[3] != NULL) {
        Ncons = atoi(argv[3]);
        if (Ncons > 100) Ncons = 100;
        if (Ncons == 0) Ncons = 1;
    } else Ncons = 1;

    CharStats stats_main;
    init_stats(&stats_main);

    share->rfile = rfile;
    share->line = NULL;
    pthread_mutex_init(&share->lock, NULL);
    pthread_cond_init(&share->cond_not_full, NULL);
    pthread_cond_init(&share->cond_not_empty, NULL);
    for (i = 0; i < Nprod; i++)
        pthread_create(&prod[i], NULL, producer, share);
    for (i = 0; i < Ncons; i++)
        pthread_create(&cons[i], NULL, consumer, share);
    printf("main continuing\n");

    int sum_c, sum_p = 0;
    for (i = 0; i < Ncons; i++) {
        rc = pthread_join(cons[i], (void **) &ret);
        printf("main: consumer_%d joined with %d\n", i, ret->i);
        sum_c += ret->i;

        accumulate_stats(&stats_main, ret->stats);

        free(ret->stats);
        free(ret);
    }
    for (i = 0; i < Nprod; i++) {
        rc = pthread_join(prod[i], (void **) &ret_producer);
        printf("main: producer_%d joined with %d\n", i, *ret_producer);
        sum_p += *ret_producer;
    }

    printf("main continuing\n");
    print_stats(&stats_main);
    printf("sum_c: %d \nsum_p: %d", sum_c, sum_p);

    pthread_exit(NULL);
    exit(0);
}
