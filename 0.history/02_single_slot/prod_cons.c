#define _XOPEN_SOURCE 700

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "char_stat.h"
#include "performance.h"

#define CHUNK_SIZE 4096
static int g_chunk_id_counter = 0;
static pthread_mutex_t g_chunk_id_lock = PTHREAD_MUTEX_INITIALIZER;
typedef struct {
    char *data;
    size_t size;
    int chunk_id;

    pthread_mutex_t lock;
    pthread_cond_t cond_not_full;
    pthread_cond_t cond_not_empty;
    int full;
    int done;
} SingleSlotBuffer;

typedef struct {
    char *data;
    size_t size;
    int chunk_id;
} Chunk;

typedef struct {
    int chunks_processed;
    size_t bytes_processed;
    CharStats *stats;
} ConsumerResult;

typedef struct {
    long start_offset;
    long end_offset;
    long current_offset;
} WorkRange;

typedef struct {
    FILE *rfile;
    int fd;
    long file_size;
} FileReader;

void init_file_reader(FileReader *reader, FILE *rfile) {
    reader->rfile = rfile;
    reader->fd = fileno(rfile);

    fseek(rfile, 0, SEEK_END);
    reader->file_size = ftell(rfile);
    fseek(rfile, 0, SEEK_SET);
}

void init_single_slot_buffer(SingleSlotBuffer *sb) {
    sb->data = NULL;
    sb->size = 0;
    sb->chunk_id = -1;
    sb->full = 0;
    sb->done = 0;

    pthread_mutex_init(&sb->lock, NULL);
    pthread_cond_init(&sb->cond_not_full, NULL);
    pthread_cond_init(&sb->cond_not_empty, NULL);
}

void destroy_single_slot_buffer(SingleSlotBuffer *sb) {
    if (sb->data != NULL) {
        free(sb->data);
        sb->data = NULL;
    }

    pthread_mutex_destroy(&sb->lock);
    pthread_cond_destroy(&sb->cond_not_full);
    pthread_cond_destroy(&sb->cond_not_empty);
}

int put_chunk_single(SingleSlotBuffer *sb, char *data, size_t size, int chunk_id) {
    pthread_mutex_lock(&sb->lock);

    while (sb->full && !sb->done) {
        pthread_cond_wait(&sb->cond_not_full, &sb->lock);
    }

    if (sb->done) {
        pthread_mutex_unlock(&sb->lock);
        return 0;
    }

    sb->data = data;
    sb->size = size;
    sb->chunk_id = chunk_id;
    sb->full = 1;

    pthread_cond_signal(&sb->cond_not_empty);
    pthread_mutex_unlock(&sb->lock);

    return 1;
}

Chunk get_chunk_single(SingleSlotBuffer *sb) {
    pthread_mutex_lock(&sb->lock);

    while (1) {
        while (!sb->full && !sb->done) {
            pthread_cond_wait(&sb->cond_not_empty, &sb->lock);
        }

        if (sb->done && !sb->full) {
            pthread_mutex_unlock(&sb->lock);
            return (Chunk){.data = NULL, .size = 0, .chunk_id = -1};
        }

        if (sb->full) {
            Chunk chunk;
            chunk.data = sb->data;
            chunk.size = sb->size;
            chunk.chunk_id = sb->chunk_id;

            sb->data = NULL;
            sb->full = 0;

            pthread_cond_signal(&sb->cond_not_full);
            pthread_mutex_unlock(&sb->lock);

            return chunk;
        }
    }
}

int log_index = 0;
pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;

typedef struct {
    FileReader *reader;
    SingleSlotBuffer *buffer;
    WorkRange range;
} ProducerArgsSingle;

void *producer_single(void *arg) {
    ProducerArgsSingle *args = (ProducerArgsSingle *)arg;
    FileReader *reader = args->reader;
    SingleSlotBuffer *buffer = args->buffer;
    WorkRange *range = &args->range;

    int *ret = malloc(sizeof(int));
    int chunks_produced = 0;

    printf("Prod_%x (Single): range [%ld - %ld]\n",
           (unsigned int)pthread_self(),
           range->start_offset, range->end_offset);

    while (range->current_offset < range->end_offset) {
        long remaining = range->end_offset - range->current_offset;
        size_t read_size = (remaining < CHUNK_SIZE) ? remaining : CHUNK_SIZE;

        char *chunk_data = malloc(read_size);
        if (chunk_data == NULL) {
            fprintf(stderr, "Memory allocation failed\n");
            break;
        }

        ssize_t bytes_read = pread(reader->fd, chunk_data,
                                   read_size, range->current_offset);

        if (bytes_read <= 0) {
            free(chunk_data);
            break;
        }

        range->current_offset += bytes_read;

        pthread_mutex_lock(&g_chunk_id_lock);
        int global_chunk_id = g_chunk_id_counter++;
        pthread_mutex_unlock(&g_chunk_id_lock);

        if (!put_chunk_single(buffer, chunk_data, bytes_read, global_chunk_id)) {
            free(chunk_data);
            break;
        }

        chunks_produced++;
    }

    printf("Prod_%x (Single): %d chunks\n",
           (unsigned int)pthread_self(), chunks_produced);

    *ret = chunks_produced;
    pthread_exit(ret);
}

void *consumer_single(void *arg) {
    SingleSlotBuffer *buffer = (SingleSlotBuffer *)arg;
    ConsumerResult *result = malloc(sizeof(ConsumerResult));

    static int consumer_counter = 0;
    pthread_mutex_lock(&log_lock);
    int my_id = consumer_counter++;
    pthread_mutex_unlock(&log_lock);

    result->chunks_processed = 0;
    result->bytes_processed = 0;
    result->stats = malloc(sizeof(CharStats));
    init_stats(result->stats);

    while (1) {
        Chunk chunk = get_chunk_single(buffer);

        if (chunk.data == NULL) {
            break;
        }

        for (size_t i = 0; i < chunk.size; i++) {
            update_stats_with_char(&chunk.data[i], result->stats);
        }

        result->chunks_processed++;
        result->bytes_processed += chunk.size;

        free(chunk.data);
    }

    printf("Cons_%x (Single): %d chunks, %zu bytes\n",
           (unsigned int)pthread_self(),
           result->chunks_processed,
           result->bytes_processed);

    pthread_exit(result);
}

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

    FileReader reader;
    SingleSlotBuffer buffer;

    init_file_reader(&reader, rfile);
    init_single_slot_buffer(&buffer);

    printf("File size: %ld bytes (%.2f MB)\n\n",
           reader.file_size,
           reader.file_size / (1024.0 * 1024.0));

    ProducerArgsSingle prod_args[100];

    for (int i = 0; i < Nprod; i++) {
        prod_args[i].reader = &reader;
        prod_args[i].buffer = &buffer;

        long chunk_size = reader.file_size / Nprod;
        prod_args[i].range.start_offset = i * chunk_size;
        prod_args[i].range.end_offset = (i == Nprod - 1)
                                        ? reader.file_size
                                        : (i + 1) * chunk_size;
        prod_args[i].range.current_offset = prod_args[i].range.start_offset;
    }

    CharStats stats_main;
    init_stats(&stats_main);

    start_timer(&timer);

    for (int i = 0; i < Nprod; i++) {
        pthread_create(&prod[i], NULL, producer_single, &prod_args[i]);
    }

    for (int i = 0; i < Ncons; i++) {
        pthread_create(&cons[i], NULL, consumer_single, &buffer);
    }

    int total_chunks = 0;
    for (int i = 0; i < Nprod; i++) {
        int *ret;
        pthread_join(prod[i], (void **)&ret);
        total_chunks += *ret;
        free(ret);
    }

    pthread_mutex_lock(&buffer.lock);
    buffer.done = 1;
    pthread_cond_broadcast(&buffer.cond_not_empty);
    pthread_mutex_unlock(&buffer.lock);

    size_t total_bytes = 0;
    for (int i = 0; i < Ncons; i++) {
        ConsumerResult *result;
        pthread_join(cons[i], (void **)&result);
        total_bytes += result->bytes_processed;
        accumulate_stats(&stats_main, result->stats);
        free(result->stats);
        free(result);
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
    print_metrics(&timer,Ncons,Nprod);
    print_stats(&stats_main);

    destroy_single_slot_buffer(&buffer);
    fclose(rfile);

    return 0;
}