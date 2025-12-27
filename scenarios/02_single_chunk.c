#define _XOPEN_SOURCE 700

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "../components/1_reader/chunk_reader.h"
#include "../components/2_buffer/single_chunk_buffer.h"
#include "../components/3_processor/char_stat.h"
#include "../components/4_statistics/timer.h"
#include "../components/common.h"

#define CHUNK_SIZE 4096

typedef struct {
    int id;
    int handledChunks;
    SingleChunkBuffer *so;
    CharStats *stats;
} ConsumerThread;

typedef struct {
    int id;
    SingleChunkBuffer *so;
    char *filename;
    ReadRange range;
} ProducerThread;

void *producer(void *arg) {
    ProducerThread *pt = (ProducerThread *)arg;
    SingleChunkBuffer *buffer = pt->so;

    FILE *file = fopen(pt->filename, "r");
    if (!file) { perror("fopen"); exit(1); }

    ChunkReader reader;
    chunk_reader_init(&reader, file, pt->range, CHUNK_SIZE);

    int count = 0;
    while (chunk_reader_has_more(&reader)) {
        DataUnit unit = chunk_reader_next(&reader);

        if (unit.data != NULL) {
            scb_put(buffer, unit);
            count++;
        }
    }

    chunk_reader_destroy(&reader);
    fclose(file);

    scb_notify_producer_finished(buffer);

    int *ret = malloc(sizeof(int));
    *ret = count;

    printf("Prod_%x: %d chunks\n", (unsigned int)pthread_self(), count);

    return ret;
}

void *consumer(void *arg) {
    ConsumerThread *ct = (ConsumerThread *)arg;
    SingleChunkBuffer *buffer = ct->so;

    int count = 0;
    DataUnit unit;

    while (1) {
        unit = scb_get(buffer);
        if (unit.data == NULL) break;

        // printf("Cons_%x: [%02d:%02d] %zu bytes\n",
        //        (unsigned int)pthread_self(), ct->id, unit.id, unit.size);

        update_stats_in_chunk((char*)unit.data, unit.size, ct->stats);

        free(unit.data);
        count++;
    }

    ct->handledChunks = count;
    printf("Cons: %d chunks\n", count);

    return NULL;
}

int main(int argc, char *argv[]) {
    pthread_t prod[100];
    pthread_t cons[100];
    int Nprod, Ncons;
    int rc;
    int *ret_producer;
    int i;
    MetricsTimer timer;

    if (argc == 1) {
        printf("usage: ./prod_cons <readfile> #Producer #Consumer\n");
        exit(0);
    }

    if (argv[2] != NULL) Nprod = atoi(argv[2]); else Nprod = 1;
    if (Nprod > 100) Nprod = 100;
    if (Nprod == 0) Nprod = 1;

    if (argv[3] != NULL) Ncons = atoi(argv[3]); else Ncons = 1;
    if (Ncons > 100) Ncons = 100;
    if (Ncons == 0) Ncons = 1;

    SingleChunkBuffer *share = malloc(sizeof(SingleChunkBuffer));
    scb_init(share, Nprod);

    CharStats stats_main;
    init_stats(&stats_main);

    start_timer(&timer);

    ConsumerThread* consumers = malloc(sizeof(ConsumerThread) * Ncons);
    ProducerThread* producers = malloc(sizeof(ProducerThread) * Nprod);

    FILE *fp = fopen(argv[1], "r");
    fseek(fp, 0, SEEK_END);
    long filesize = ftell(fp);
    fclose(fp);

    long chunk_size_file = filesize / Nprod;

    for (i = 0; i < Nprod; i++) {
        producers[i].id = i;
        producers[i].so = share;
        producers[i].filename = argv[1];

        producers[i].range.start = i * chunk_size_file;
        producers[i].range.end = (i == Nprod - 1) ? filesize : (i + 1) * chunk_size_file;

        pthread_create(&prod[i], NULL, producer, &producers[i]);
    }

    for (i = 0; i < Ncons; i++) {
        consumers[i].handledChunks = 0;
        consumers[i].so = share;
        consumers[i].stats = malloc(sizeof(CharStats));
        init_stats(consumers[i].stats);
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
        free(ret_producer);
    }
    free(producers);

    stop_timer(&timer);
    scb_destroy(share);
    free(share);

    printf("main continuing\n");
    print_stats(&stats_main);

    printf("sum_c: %d \nsum_p: %d", sum_c, sum_p);
    print_metrics(&timer, Ncons, Nprod);

    pthread_exit(NULL);
}