#define _XOPEN_SOURCE 700

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include "char_stat.h"
#include "performance.h"

#define BUFFER_SIZE (1024 * 1024)

typedef struct {
    int thread_id;
    int fd;
    off_t start_offset;
    off_t end_offset;
    long long *result_bytes;
} FileIndicator;


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

typedef struct {
    pthread_t thread;
    so_t *so;
    FileIndicator *file_indicator;
} ProdThread;

typedef struct ret_from_thread {
    int i;
    CharStats *stats;
} RetFromThread;

void* read_file_chunk(void* arg) {
    FileIndicator* data = (FileIndicator*)arg;

    char* buffer = (char*)malloc(BUFFER_SIZE);
    if (buffer == NULL) {
        fprintf(stderr, "Thread %d: Failed to allocate buffer\n", data->thread_id);
        *(data->result_bytes) = 0;
        return NULL;
    }

    off_t current_offset = data->start_offset;
    long long total_bytes_read_by_thread = 0;

    while (current_offset < data->end_offset) {

        off_t remaining_in_section = data->end_offset - current_offset;
        size_t bytes_to_read = (remaining_in_section < BUFFER_SIZE) ?
                               (size_t)remaining_in_section : BUFFER_SIZE;

        ssize_t bytes_read = pread(data->fd,
                                   buffer,
                                   bytes_to_read,
                                   current_offset);

        if (bytes_read == -1) {
            fprintf(stderr, "Thread %d: Error reading file at offset %lld: %s\n",
                    data->thread_id, (long long)current_offset, strerror(errno));
            break;
        }

        if (bytes_read == 0) {
            break;
        }


        total_bytes_read_by_thread += bytes_read;
        current_offset += bytes_read;
    }

    free(buffer);
    *(data->result_bytes) = total_bytes_read_by_thread;


    return NULL;
}

void *producer(void *arg) {
    ProdThread* prod_thread = (ProdThread*)arg;
    so_t* so = &prod_thread->so;
    int *ret = malloc(sizeof(int));
    int i = 0;
    char *line = NULL;
    size_t len = 0;
    ssize_t read = 0;

    while (1) {
        read = read_file_chunk(&(prod_thread->file_indicator));
        pthread_mutex_lock(&so->lock);

        while (so->full && !so->eof) {
            pthread_cond_wait(&so->cond_not_full, &so->lock);
        }

        if (so->eof) {
            pthread_mutex_unlock(&so->lock);
            break;
        }

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
void *producer(void *arg) {
    ProdThread* prod_thread = (ProdThread*)arg;
    so_t* so = prod_thread->so;
    FileIndicator* data = prod_thread->file_indicator;

    int *ret_lines = malloc(sizeof(int));
    *ret_lines = 0;

    char* buffer = (char*)malloc(BUFFER_SIZE);
    if (buffer == NULL) {
        fprintf(stderr, "Thread %d: Failed to allocate buffer\n", data->thread_id);
        pthread_exit(ret_lines);
    }

    off_t current_offset = data->start_offset;
    long long total_bytes_read_by_thread = 0;

    char *partial_line = NULL;

    while (current_offset < data->end_offset) {
        off_t remaining_in_section = data->end_offset - current_offset;
        size_t bytes_to_read = (remaining_in_section < BUFFER_SIZE) ?
                               (size_t)remaining_in_section : BUFFER_SIZE;

        ssize_t bytes_read = pread(data->fd, buffer, bytes_to_read, current_offset);

        if (bytes_read <= 0) break;

        char *buf_ptr = buffer;
        char *end_of_buf = buffer + bytes_read;

        if (partial_line) {
            char *newline_pos = memchr(buf_ptr, '\n', bytes_read);

            if (newline_pos) {
                int line_part_len = newline_pos - buf_ptr + 1;
                int old_len = strlen(partial_line);
                char *full_line = malloc(old_len + line_part_len + 1);

                memcpy(full_line, partial_line, old_len);
                memcpy(full_line + old_len, buf_ptr, line_part_len);
                full_line[old_len + line_part_len] = '\0';

                put_line_to_queue(so, full_line, (*ret_lines)++);

                free(partial_line);
                partial_line = NULL;
                buf_ptr = newline_pos + 1;
            } else {
                int old_len = strlen(partial_line);
                partial_line = realloc(partial_line, old_len + bytes_read + 1);
                memcpy(partial_line + old_len, buf_ptr, bytes_read);
                partial_line[old_len + bytes_read] = '\0';
                buf_ptr = end_of_buf;
            }
        }

        while (buf_ptr < end_of_buf) {
            char *newline_pos = memchr(buf_ptr, '\n', end_of_buf - buf_ptr);

            if (newline_pos) {
                int line_len = newline_pos - buf_ptr + 1;
                char *line = malloc(line_len + 1);
                memcpy(line, buf_ptr, line_len);
                line[line_len] = '\0';

                put_line_to_queue(so, line, (*ret_lines)++);

                buf_ptr = newline_pos + 1;
            } else {
                int partial_len = end_of_buf - buf_ptr;
                partial_line = malloc(partial_len + 1);
                memcpy(partial_line, buf_ptr, partial_len);
                partial_line[partial_len] = '\0';
                break;
            }
        }

        total_bytes_read_by_thread += bytes_read;
        current_offset += bytes_read;
    }

    if (partial_line) free(partial_line);

    free(buffer);

    if (data->result_bytes) {
        *(data->result_bytes) = total_bytes_read_by_thread;
    }

    printf("Prod_%x: %d lines\n", (unsigned int) pthread_self(), *ret_lines);
    pthread_exit(ret_lines);
}
void *consumer(void *arg) {
    so_t *so = arg;
    RetFromThread *ret = malloc(sizeof(RetFromThread));
    int i = 0;
    int len;
    char *line;
    int linenum;

    CharStats *stats = malloc(sizeof(CharStats));
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
        linenum = so->linenum;
        so->line = NULL;
        so->full = 0;

        pthread_cond_signal(&so->cond_not_full);
        pthread_mutex_unlock(&so->lock);

        len = strlen(line);
        printf("Cons_%x: [%02d:%02d] %s",
               (unsigned int) pthread_self(), i, linenum, line);
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
    pthread_t cons[100];
    int Nprod, Ncons;
    int rc;
    long t;
    RetFromThread *ret;
    int *ret_producer;
    int i;
    FILE *rfile;
    MetricsTimer timer;

    if (argc == 1) {
        printf("usage: ./prod_cons <readfile> #Producer #Consumer\n");
        exit(0);
    }
    so_t *share = malloc(sizeof(so_t));
    memset(share, 0, sizeof(so_t));
    int fd = open((char *) argv[1], O_RDONLY);
    if (fd == -1) {
        perror("Failed to open file");
        return 1;
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

    ProdThread prod_threads[Nprod];
    FileIndicator file_indicators[Nprod];

    struct stat file_stat;
    if (fstat(fd, &file_stat) == -1) {
        perror("Failed to get file stats");
        close(fd);
        return 1;
    }
    off_t total_file_size = file_stat.st_size;
    printf("Total file size: %lld bytes\n", (long long) total_file_size);

    off_t chunk_size_per_thread = total_file_size / Nprod;
    off_t current_offset = 0;

    CharStats stats_main;
    init_stats(&stats_main);

    share->line = NULL;
    pthread_mutex_init(&share->lock, NULL);
    pthread_cond_init(&share->cond_not_full, NULL);
    pthread_cond_init(&share->cond_not_empty, NULL);

    start_timer(&timer);

    for (int i = 0; i < Nprod; ++i) {
        file_indicators[i].thread_id = i;
        file_indicators[i].fd = fd;
        file_indicators[i].start_offset = current_offset;
        file_indicators[i].result_bytes = &thread_results[i];

        if (i == Nprod - 1) {
            file_indicators[i].end_offset = total_file_size;
        } else {
            file_indicators[i].end_offset = current_offset + chunk_size_per_thread;
        }

        printf("  Thread %d: %lld (start) -> %lld (end) [Size: %lld bytes]\n",
               i,
               (long long)file_indicators[i].start_offset,
               (long long)file_indicators[i].end_offset,
               (long long)(file_indicators[i].end_offset - file_indicators[i].start_offset));

        current_offset = file_indicators[i].end_offset;

        prod_threads[i].so = share;
        prod_threads[i].file_indicator = &file_indicators[i];

        pthread_create(&prod_threads[i].thread, NULL, producer, &prod_threads[i]);
    }

    for (i = 0; i < Ncons; i++)
        pthread_create(&cons[i], NULL, consumer, share);
    printf("main continuing\n");

    int sum_c = 0;
    int sum_p = 0;
    for (i = 0; i < Ncons; i++) {
        rc = pthread_join(cons[i], (void **) &ret);
        printf("main: consumer_%d joined with %d\n", i, ret->i);
        sum_c += ret->i;

        accumulate_stats(&stats_main, ret->stats);

        free(ret->stats);
        free(ret);
    }
    for (i = 0; i < Nprod; i++) {
        rc = pthread_join(prod_threads[i].thread, (void **) &ret_producer);
        printf("main: producer_%d joined with %d lines\n", i, *ret_producer);
        sum_p += *ret_producer;
        free(ret_producer);
    }

    stop_timer(&timer);

    printf("main continuing\n");
    print_stats(&stats_main);
    printf("sum_c: %d \nsum_p: %d", sum_c, sum_p);
    print_metrics(&timer, Ncons, Nprod);

    pthread_exit(NULL);
    exit(0);
}
