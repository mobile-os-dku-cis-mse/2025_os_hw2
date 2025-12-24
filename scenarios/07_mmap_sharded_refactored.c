#define _XOPEN_SOURCE 700

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "../components/1_reader/mmap_reader.h"
#include "../components/2_buffer/sharded_buffer.h"
#include "../components/3_processor/char_stat.h"
#include "../components/4_statistics/timer.h"
#include "../components/common.h"

#define CHUNK_SIZE 4096

typedef struct {
    int id;
    int handledChunks;
    ShardedBuffer *so;
    CharStats *stats;
} ConsumerThread;

typedef struct {
    int id;
    ShardedBuffer *so;
    char *filename;
    ReadRange range;
    MmapReader reader;
} ProducerThread;

void *producer(void *arg) {
    ProducerThread *pt = (ProducerThread *)arg;
    ShardedBuffer *buffer = pt->so;

    FILE *file = fopen(pt->filename, "r");
    if (!file) { perror("fopen"); exit(1); }

    mmap_reader_init(&pt->reader, file, pt->range, CHUNK_SIZE);

    int count = 0;
    while (mmap_reader_has_more(&pt->reader)) {
        DataUnit unit = mmap_reader_next(&pt->reader);

        if (unit.data != NULL) {
            sb_put(buffer, unit, pt->id);
            count++;
        }
    }

    fclose(file); 
    
    sb_notify_producer_finished(buffer);

    int *ret = malloc(sizeof(int));
    *ret = count;

    printf("Prod_%x: %d chunks\n", (unsigned int)pthread_self(), count);
    return ret;
}

void *consumer(void *arg) {
    ConsumerThread *ct = (ConsumerThread *)arg;
    ShardedBuffer *buffer = ct->so;

    int count = 0;
    DataUnit unit;

    while (1) {
        unit = sb_get(buffer, ct->id);
        
        if (unit.data == NULL) break; 

        update_stats_in_chunk((char*)unit.data, unit.size, ct->stats);

        count++;
    }

    ct->handledChunks = count;
    printf("Cons: %d chunks\n", count);

    return NULL;
}

int main(int argc, char *argv[]) {
    pthread_t prod[100];
    pthread_t cons[100];
    int Nprod, Ncons, BufferSize, NShards;
    int rc;
    int *ret_producer;
    int i;
    MetricsTimer timer;

    if (argc == 1) {
        printf("usage: ./prod_cons <readfile> #Producer #Consumer [BufferSize] [NShards]\n");
        exit(0);
    }

    if (argv[2] != NULL) Nprod = atoi(argv[2]); else Nprod = 1;
    if (Nprod > 100) Nprod = 100; if (Nprod == 0) Nprod = 1;

    if (argv[3] != NULL) Ncons = atoi(argv[3]); else Ncons = 1;
    if (Ncons > 100) Ncons = 100; if (Ncons == 0) Ncons = 1;

    if (argc > 4 && argv[4] != NULL) BufferSize = atoi(argv[4]); else BufferSize = 10;
    
    if (argc > 5 && argv[5] != NULL) NShards = atoi(argv[5]); else NShards = Nprod;
    if (NShards <= 0) NShards = 1;

    ShardedBuffer *share = malloc(sizeof(ShardedBuffer));
    sb_init(share, NShards, BufferSize, Nprod);

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
        pthread_join(cons[i], NULL);
        printf("main: consumer_%d joined with %d\n", consumers[i].id, consumers[i].handledChunks);
        sum_c += consumers[i].handledChunks;
        accumulate_stats(&stats_main, consumers[i].stats);
        free(consumers[i].stats);
    }
    free(consumers);

    for (i = 0; i < Nprod; i++) {
        pthread_join(prod[i], (void **) &ret_producer);
        printf("main: producer_%d joined with %d\n", i, *ret_producer);
        sum_p += *ret_producer;
        free(ret_producer);
        mmap_reader_destroy(&producers[i].reader);
    }
    free(producers);

    stop_timer(&timer);
    
    sb_destroy(share);
    free(share);

    printf("main continuing\n");
    print_stats(&stats_main);
    printf("sum_c: %d \nsum_p: %d", sum_c, sum_p);
    print_metrics(&timer, Ncons, Nprod);

    pthread_exit(NULL);
}