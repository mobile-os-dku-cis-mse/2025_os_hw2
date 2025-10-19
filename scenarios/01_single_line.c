#define _XOPEN_SOURCE 700

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "../components/3_processor/char_stat.h"
#include "../components/4_statistics/timer.h"
#include "../components/2_buffers/single_line.h"

int main(int argc, char *argv[]) {
    pthread_t prod[100];
    pthread_t cons[100];
    int Nprod, Ncons;
    int rc;
    int *ret_producer;
    int i;
    FILE *rfile;
    MetricsTimer timer;

    if (argc == 1) {
        printf("usage: ./prod_cons <readfile> #Producer #Consumer\n");
        exit(0);
    }
    SingleLineBuffer *share = malloc(sizeof(SingleLineBuffer));
    memset(share, 0, sizeof(SingleLineBuffer));
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

    start_timer(&timer);

    ConsumerThread* consumers = malloc(sizeof(ConsumerThread) * Ncons);

    for (i = 0; i < Nprod; i++)
        pthread_create(&prod[i], NULL, producer, share);
    for (i = 0; i < Ncons; i++) {
        init_consumer_thread(&consumers[i], share);
        consumers[i].id = i;

        pthread_create(&cons[i], NULL, consumer, &consumers[i]);
    }
    printf("main continuing\n");

    int sum_c = 0;
    int sum_p = 0;
    for (i = 0; i < Ncons; i++) {
        rc = pthread_join(cons[i], NULL);
        printf("main: consumer_%d joined with %d\n", consumers[i].id, consumers[i].handledChunks);
        sum_c += consumers[i].handledChunks;

        accumulate_stats(&stats_main, consumers[i].stats);

        free(consumers[i].stats);
    }
    free(consumers);

    for (i = 0; i < Nprod; i++) {
        rc = pthread_join(prod[i], (void **) &ret_producer);
        printf("main: producer_%d joined with %d\n", i, *ret_producer);
        sum_p += *ret_producer;
    }

    stop_timer(&timer);

    printf("main continuing\n");
    print_stats(&stats_main);
    printf("sum_c: %d \nsum_p: %d", sum_c, sum_p);
    print_metrics(&timer, Ncons, Nprod);

    pthread_exit(NULL);
    exit(0);
}
