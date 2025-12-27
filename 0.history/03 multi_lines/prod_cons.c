#define _XOPEN_SOURCE 700

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "char_stat.h"
#include "performance.h"
#define CHUNK_SIZE 4096

typedef struct ret_from_thread {
    int i;
    CharStats *stats;
} RetFromThread;

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

typedef struct {
    char *line;
    int linenum;
    pthread_mutex_t lock;
    pthread_cond_t cond_not_full;
    pthread_cond_t cond_not_empty;
    int full;
    int global_linenum;
    int done;
} SharedBuffer;

typedef struct {
    FileReader *reader;
    SharedBuffer *buffer;
    WorkRange range;
} ProducerArgs;

void *producer(void *arg) {
    ProducerArgs *args = (ProducerArgs *) arg;
    FileReader *reader = args->reader;
    SharedBuffer *buffer = args->buffer;
    WorkRange *range = &args->range;

    int *ret = malloc(sizeof(int));
    int lines_produced = 0;

    char *leftover = NULL;
    size_t leftover_len = 0;

    printf("Prod_%x: assigned range [%ld - %ld] (%.2f GB)\n",
           (unsigned int) pthread_self(),
           range->start_offset,
           range->end_offset,
           (range->end_offset - range->start_offset) / (1024.0 * 1024.0 * 1024.0));

    if (range->start_offset > 0) {
        char ch;
        while (range->current_offset < range->end_offset &&
               pread(reader->fd, &ch, 1, range->current_offset) > 0) {
            range->current_offset++;
            if (ch == '\n') break;
        }
    }

    while (range->current_offset < range->end_offset) {
        long remainder_in_range = range->end_offset - range->current_offset;
        size_t read_size = (remainder_in_range < CHUNK_SIZE)
                               ? remainder_in_range
                               : CHUNK_SIZE;

        char *chunk_buffer;
        size_t buffer_size = 0;

        if (leftover != NULL && leftover_len > 0) {
            chunk_buffer = malloc(leftover_len + read_size + 1);
            memcpy(chunk_buffer, leftover, leftover_len);
            buffer_size = leftover_len;
            free(leftover);
            leftover = NULL;
            leftover_len = 0;
        } else {
            chunk_buffer = malloc(read_size + 1);
        }

        ssize_t bytes_read = pread(reader->fd,
                                   chunk_buffer + buffer_size,
                                   read_size,
                                   range->current_offset);

        if (bytes_read <= 0) {
            free(chunk_buffer);
            break;
        }

        range->current_offset += bytes_read;
        buffer_size += bytes_read;
        chunk_buffer[buffer_size] = '\0';

        char *parse_end = chunk_buffer + buffer_size;
        char *last_newline = NULL;

        for (char *p = parse_end - 1; p >= chunk_buffer; p--) {
            if (*p == '\n') {
                last_newline = p;
                break;
            }
        }

        int is_my_last_chunk = (range->current_offset >= range->end_offset);

        if (last_newline != NULL) {
            size_t complete_size = last_newline - chunk_buffer + 1;

            leftover_len = buffer_size - complete_size;
            if (leftover_len > 0) {
                leftover = malloc(leftover_len + 1);
                memcpy(leftover, last_newline + 1, leftover_len);
                leftover[leftover_len] = '\0';
            }

            chunk_buffer[complete_size] = '\0';
        } else if (!is_my_last_chunk) {
            leftover = chunk_buffer;
            leftover_len = buffer_size;
            continue;
        }

        char *line_start = chunk_buffer;
        char *line_end;

        while ((line_end = strchr(line_start, '\n')) != NULL) {
            size_t line_len = line_end - line_start + 1;
            char *my_line = malloc(line_len + 1);
            memcpy(my_line, line_start, line_len);
            my_line[line_len] = '\0';

            pthread_mutex_lock(&buffer->lock);
            while (buffer->full && !buffer->done) {
                pthread_cond_wait(&buffer->cond_not_full, &buffer->lock);
            }

            if (buffer->done) {
                pthread_mutex_unlock(&buffer->lock);
                free(my_line);
                free(chunk_buffer);
                goto cleanup;
            }

            buffer->linenum = buffer->global_linenum++;
            buffer->line = my_line;
            buffer->full = 1;

            pthread_cond_signal(&buffer->cond_not_empty);
            pthread_mutex_unlock(&buffer->lock);

            lines_produced++;
            line_start = line_end + 1;
        }

        free(chunk_buffer);
    }

    if (leftover != NULL && leftover_len > 0) {
        pthread_mutex_lock(&buffer->lock);
        while (buffer->full && !buffer->done) {
            pthread_cond_wait(&buffer->cond_not_full, &buffer->lock);
        }
        if (!buffer->done) {
            buffer->linenum = buffer->global_linenum++;
            buffer->line = leftover;
            buffer->full = 1;
            lines_produced++;
            pthread_cond_signal(&buffer->cond_not_empty);
        } else {
            free(leftover);
        }
        pthread_mutex_unlock(&buffer->lock);
    }

cleanup:
    printf("Prod_%x: finished with %d lines\n",
           (unsigned int) pthread_self(), lines_produced);
    *ret = lines_produced;
    pthread_exit(ret);
}

void *consumer(void *arg) {
    SharedBuffer *buffer = (SharedBuffer *) arg;
    RetFromThread *ret = malloc(sizeof(RetFromThread));
    int i = 0;

    CharStats *stats = malloc(sizeof(CharStats));
    init_stats(stats);

    while (1) {
        pthread_mutex_lock(&buffer->lock);

        while (buffer->full == 0 && !buffer->done) {
            pthread_cond_wait(&buffer->cond_not_empty, &buffer->lock);
        }

        if (buffer->done && !buffer->full) {
            pthread_mutex_unlock(&buffer->lock);
            break;
        }

        char *line = buffer->line;
        int linenum = buffer->linenum;
        buffer->line = NULL;
        buffer->full = 0;

        pthread_cond_signal(&buffer->cond_not_full);
        pthread_mutex_unlock(&buffer->lock);

        printf("Cons_%x: [%02d:%02d] %s",
               (unsigned int) pthread_self(), i, linenum, line);
        update_stats_in_line(line, stats);
        free(line);
        i++;
    }

    printf("Cons_%x: %d lines\n", (unsigned int) pthread_self(), i);

    ret->i = i;
    ret->stats = stats;
    pthread_exit(ret);
}

WorkRange calculate_work_range(FileReader *reader, int producer_id, int num_producers) {
    WorkRange range;

    long chunk_size = reader->file_size / num_producers;

    range.start_offset = producer_id * chunk_size;

    if (producer_id == num_producers - 1) {
        range.end_offset = reader->file_size;
    } else {
        range.end_offset = (producer_id + 1) * chunk_size;
    }

    if (producer_id > 0 && range.start_offset < reader->file_size) {
        char buffer[4096];
        ssize_t n = pread(reader->fd, buffer, sizeof(buffer), range.start_offset);

        if (n > 0) {
            for (ssize_t i = 0; i < n; i++) {
                if (buffer[i] == '\n') {
                    range.start_offset += i + 1;
                    break;
                }
            }
        }
    }

    if (producer_id < num_producers - 1 && range.end_offset < reader->file_size) {
        char buffer[4096];
        ssize_t n = pread(reader->fd, buffer, sizeof(buffer), range.end_offset);

        if (n > 0) {
            for (ssize_t i = 0; i < n; i++) {
                if (buffer[i] == '\n') {
                    range.end_offset += i + 1;
                    break;
                }
            }
        }
    }

    range.current_offset = range.start_offset;

    return range;
}

void init_file_reader(FileReader *reader, FILE *rfile) {
    reader->rfile = rfile;
    reader->fd = fileno(rfile);

    fseek(rfile, 0, SEEK_END);
    reader->file_size = ftell(rfile);
    fseek(rfile, 0, SEEK_SET);
}

void init_shared_buffer(SharedBuffer *buffer) {
    buffer->line = NULL;
    buffer->linenum = 0;
    buffer->full = 0;
    buffer->global_linenum = 0;
    buffer->done = 0;

    pthread_mutex_init(&buffer->lock, NULL);
    pthread_cond_init(&buffer->cond_not_full, NULL);
    pthread_cond_init(&buffer->cond_not_empty, NULL);
}

void destroy_shared_buffer(SharedBuffer *buffer) {
    if (buffer->line) {
        free(buffer->line);
        buffer->line = NULL;
    }

    pthread_mutex_destroy(&buffer->lock);
    pthread_cond_destroy(&buffer->cond_not_full);
    pthread_cond_destroy(&buffer->cond_not_empty);
}

int main(int argc, char *argv[]) {
    pthread_t prod[100];
    pthread_t cons[100];
    int Nprod, Ncons;
    FILE *rfile;
    MetricsTimer timer;

    if (argc == 1) {
        printf("usage: ./prod_cons <readfile> #Producer #Consumer\n");
        printf("example: ./prod_cons data.txt 4 2\n");
        exit(0);
    }

    rfile = fopen(argv[1], "r");
    if (rfile == NULL) {
        perror("Failed to open file");
        exit(1);
    }

    if (argc > 2 && argv[2] != NULL) {
        Nprod = atoi(argv[2]);
        if (Nprod > 100) Nprod = 100;
        if (Nprod <= 0) Nprod = 1;
    } else {
        Nprod = 1;
    }

    if (argc > 3 && argv[3] != NULL) {
        Ncons = atoi(argv[3]);
        if (Ncons > 100) Ncons = 100;
        if (Ncons <= 0) Ncons = 1;
    } else {
        Ncons = 1;
    }

    printf("=== Configuration ===\n");
    printf("File: %s\n", argv[1]);
    printf("Producers: %d\n", Nprod);
    printf("Consumers: %d\n", Ncons);
    printf("\n");

    FileReader reader;
    init_file_reader(&reader, rfile);

    printf("=== File Info ===\n");
    printf("File size: %ld bytes (%.2f MB)\n",
           reader.file_size,
           reader.file_size / (1024.0 * 1024.0));
    printf("\n");

    SharedBuffer buffer;
    init_shared_buffer(&buffer);

    ProducerArgs prod_args[100];

    printf("=== Work Distribution ===\n");
    for (int i = 0; i < Nprod; i++) {
        prod_args[i].reader = &reader;
        prod_args[i].buffer = &buffer;
        prod_args[i].range = calculate_work_range(&reader, i, Nprod);

        long range_size = prod_args[i].range.end_offset -
                          prod_args[i].range.start_offset;

        printf("Producer %d: [%10ld - %10ld] = %10ld bytes (%.2f MB)\n",
               i,
               prod_args[i].range.start_offset,
               prod_args[i].range.end_offset,
               range_size,
               range_size / (1024.0 * 1024.0));
    }
    printf("\n");

    CharStats stats_main;
    init_stats(&stats_main);

    start_timer(&timer);

    printf("=== Starting Threads ===\n");
    for (int i = 0; i < Nprod; i++) {
        int rc = pthread_create(&prod[i], NULL, producer, &prod_args[i]);
        if (rc) {
            fprintf(stderr, "Error creating producer %d: %d\n", i, rc);
            exit(1);
        }
        printf("Producer %d created (thread %x)\n", i, (unsigned int) prod[i]);
    }

    for (int i = 0; i < Ncons; i++) {
        int rc = pthread_create(&cons[i], NULL, consumer, &buffer);
        if (rc) {
            fprintf(stderr, "Error creating consumer %d: %d\n", i, rc);
            exit(1);
        }
        printf("Consumer %d created (thread %x)\n", i, (unsigned int) cons[i]);
    }
    printf("\n");

    printf("=== Processing... ===\n");

    int sum_p = 0;
    printf("\n=== Waiting for Producers ===\n");
    for (int i = 0; i < Nprod; i++) {
        int *ret;
        int rc = pthread_join(prod[i], (void **) &ret);
        if (rc) {
            fprintf(stderr, "Error joining producer %d: %d\n", i, rc);
        } else {
            printf("Producer %d finished: %d lines\n", i, *ret);
            sum_p += *ret;
            free(ret);
        }
    }
    printf("Total lines produced: %d\n", sum_p);
    printf("\n");

    pthread_mutex_lock(&buffer.lock);
    buffer.done = 1;
    pthread_cond_broadcast(&buffer.cond_not_empty);
    pthread_mutex_unlock(&buffer.lock);

    int sum_c = 0;
    printf("=== Waiting for Consumers ===\n");
    for (int i = 0; i < Ncons; i++) {
        RetFromThread *ret;
        int rc = pthread_join(cons[i], (void **) &ret);
        if (rc) {
            fprintf(stderr, "Error joining consumer %d: %d\n", i, rc);
        } else {
            printf("Consumer %d finished: %d lines\n", i, ret->i);
            sum_c += ret->i;

            accumulate_stats(&stats_main, ret->stats);

            free(ret->stats);
            free(ret);
        }
    }
    printf("Total lines consumed: %d\n", sum_c);
    printf("\n");

    stop_timer(&timer);

    printf("========================================\n");
    printf("=== Final Results ===\n");
    printf("========================================\n");

    printf("\n--- Line Statistics ---\n");
    printf("Lines produced: %d\n", sum_p);
    printf("Lines consumed: %d\n", sum_c);

    if (sum_p != sum_c) {
        printf("WARNING: Mismatch between produced and consumed lines!\n");
    }

    printf("\n--- Character Statistics ---\n");
    print_stats(&stats_main);

    printf("\n--- Performance Metrics ---\n");
    print_metrics(&timer, Ncons, Nprod);

    destroy_shared_buffer(&buffer);
    fclose(rfile);

    printf("\nProgram completed successfully.\n");
    return 0;
}
