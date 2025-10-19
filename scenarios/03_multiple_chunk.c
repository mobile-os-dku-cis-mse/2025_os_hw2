#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../components/3_processor/char_stat.h"
#include "../components/4_statistics/timer.h"
#include "../components/2_buffer/multiple_chunk.h"

int main(int argc, char *argv[]) {
    pthread_t prod[100];
    pthread_t cons[100];
    int Nprod, Ncons;
    FILE *rfile;
    MetricsTimer timer;

    if (argc < 2) {
        printf("usage: ./prod_cons_single <file> [#prod] [#cons]\n");
        exit(0);
    }

    rfile = fopen(argv[1], "r");
    if (!rfile) {
        perror("fopen");
        exit(1);
    }

    Nprod = (argc > 2) ? atoi(argv[2]) : 1;
    Ncons = (argc > 3) ? atoi(argv[3]) : 1;
    if (Nprod > 100) Nprod = 100;
    if (Nprod <= 0) Nprod = 1;
    if (Ncons > 100) Ncons = 100;
    if (Ncons <= 0) Ncons = 1;

    start_timer(&timer);

    FileReader file;
    MultipleChunkBuffer* buffer = malloc(sizeof(MultipleChunkBuffer)*Nprod);

    init_file_reader(&file, rfile);

    CharStats stats_main;
    init_stats(&stats_main);

    ProducerThread *producers = malloc(sizeof(ProducerThread) * Nprod);
    long chunk_size = file.file_size / Nprod;
    for (int i = 0; i < Nprod; i++) {
        WorkRange *work = malloc(sizeof(WorkRange));
        work ->start_offset = i * chunk_size;
        work ->end_offset = (i == Nprod - 1) ? file.file_size : (i + 1) * chunk_size;
        work ->current_offset = work->start_offset;

        init_multiple_slot_buffer(&buffer[i]);

        producers[i].file = &file;
        producers[i].so = &buffer[i];
        producers[i].work_range = work;
        producers[i].id = i;
        pthread_create(&prod[i], NULL, producer, &producers[i]);
    }

    ConsumerThread *consumers = malloc(sizeof(ConsumerThread) * Ncons);
    for (int i = 0; i < Ncons; i++) {
        init_consumer_thread(&consumers[i], &buffer[i]);
        consumers[i].id = i;

        pthread_create(&cons[i], NULL, consumer, &consumers[i]);
    }

    int total_chunks = 0;
    for (int i = 0; i < Nprod; i++) {
        pthread_join(prod[i], NULL);
        total_chunks += producers[i].handledChunks;
        free(producers[i].work_range);
    }

    size_t total_bytes = 0;
    for (int i = 0; i < Ncons; i++) {
        pthread_join(cons[i], NULL);
        total_bytes += consumers[i].handledBytes;
        accumulate_stats(&stats_main, consumers[i].stats);
        free(consumers[i].stats);
    }

    stop_timer(&timer);
    printf("========================================\n");
    printf("=== SINGLE SLOT BUFFER VERSION ===\n");
    printf("========================================\n");
    printf("File: %s\n", argv[1]);
    printf("Producers: %d, Consumers: %d\n", Nprod, Ncons);
    printf("Buffer: 1 slot (blocking)\n");
    printf("Chunk size: %d bytes\n\n", CHUNK_SIZE);

    printf("\n=== SINGLE SLOT Results ===\n");
    printf("Total chunks: %d\n", total_chunks);
    printf("Total bytes: %zu\n", total_bytes);
    print_metrics(&timer, Ncons, Nprod);
    print_stats(&stats_main);

    free(buffer);
    free(consumers);
    free(producers);
    fclose(rfile);

    return 0;
}