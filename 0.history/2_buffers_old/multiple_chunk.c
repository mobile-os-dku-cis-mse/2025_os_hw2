#define _XOPEN_SOURCE 700

#include "multiple_chunk.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "../3_processor/char_stat.h"

void init_producer_thread(ProducerThread *thread, MultipleChunkBuffer *buffer, FileReader *file, WorkRange *work) {
    memset(thread, 0, sizeof(ProducerThread));
    thread->handledChunks = 0;
    thread->handledBytes = 0;
    thread->so = buffer;
    thread->file = file;
    thread->work_range = work;
}

void init_consumer_thread(ConsumerThread *thread, MultipleChunkBuffer *buffer) {
    memset(thread, 0, sizeof(ConsumerThread));
    thread->handledChunks = 0;
    thread->handledBytes = 0;
    thread->so = buffer;
    thread->stats = malloc(sizeof(CharStats));

    init_stats(thread->stats);
}

void init_file_reader(FileReader *reader, FILE *rfile) {
    reader->rfile = rfile;
    reader->fd = fileno(rfile);

    fseek(rfile, 0, SEEK_END);
    reader->file_size = ftell(rfile);
    fseek(rfile, 0, SEEK_SET);
}

void init_multiple_slot_buffer(MultipleChunkBuffer *buffer) {
    buffer->chunk = NULL;
    buffer->full = 0;
    buffer->done = 0;

    pthread_mutex_init(&buffer->lock, NULL);
    pthread_cond_init(&buffer->cond_not_full, NULL);
    pthread_cond_init(&buffer->cond_not_empty, NULL);
}

void *producer(void *arg) {
    ProducerThread *producer = (ProducerThread *) arg;
    FileReader *reader = producer->file;
    MultipleChunkBuffer *buffer = producer->so;
    WorkRange *work_range = producer->work_range;

    while (work_range->current_offset < work_range->end_offset) {
        long remaining = work_range->end_offset - work_range->current_offset;
        size_t read_size = (remaining < CHUNK_SIZE) ? remaining : CHUNK_SIZE;
        Chunk *chunk_data = malloc(sizeof(Chunk));

        if (chunk_data == NULL) {
            fprintf(stderr, "Memory allocation failed\n");
            break;
        }

        chunk_data->data = malloc(read_size);
        if (chunk_data->data == NULL) {
            free(chunk_data);
            break;
        }

        ssize_t bytes_read = pread(reader->fd, chunk_data->data, read_size, work_range->current_offset);
        if (bytes_read <= 0) {
            free(chunk_data);
            break;
        }

        chunk_data->size = bytes_read;
        work_range->current_offset += bytes_read;

        pthread_mutex_lock(&buffer->lock);

        while (buffer->full && !buffer->done) {
            pthread_cond_wait(&buffer->cond_not_full, &buffer->lock);
        }

        if (buffer->done) {
            pthread_mutex_unlock(&buffer->lock);
            return 0;
        }

        buffer->chunk = chunk_data;
        buffer->done = 0;
        buffer->full = 1;

        pthread_cond_signal(&buffer->cond_not_empty);
        pthread_mutex_unlock(&buffer->lock);

        producer->handledChunks++;
        producer->handledBytes += bytes_read;
    }

    printf("Prod_%x : %ld chunks, %ld bytes\n", producer->id, producer->handledChunks, producer->handledBytes);

    pthread_mutex_lock(&buffer->lock);
    buffer->done = 1;
    pthread_cond_broadcast(&buffer->cond_not_empty);
    pthread_mutex_unlock(&buffer->lock);
    pthread_exit(NULL);
}

void *consumer(void *arg) {
    ConsumerThread *consumer = arg;
    MultipleChunkBuffer *buffer = consumer->so;

    consumer->handledBytes = 0;
    consumer->handledChunks = 0;

    while (1) {
        Chunk *chunk;

        while (1) {
            pthread_mutex_lock(&buffer->lock);

            while (!buffer->full && !buffer->done) {
                pthread_cond_wait(&buffer->cond_not_empty, &buffer->lock);
            }

            if (buffer->done && !buffer->full) {
                pthread_mutex_unlock(&buffer->lock);
                chunk = NULL;
                break;
            }

            if (buffer->full) {
                chunk = buffer->chunk;

                buffer->chunk = NULL;
                buffer->full = 0;

                pthread_cond_signal(&buffer->cond_not_full);
                pthread_mutex_unlock(&buffer->lock);
                break;
            }
        }

        if (chunk == NULL) break;

        update_stats_in_chunk(chunk->data, consumer->stats);

        consumer->handledChunks++;
        consumer->handledBytes += chunk->size;

        free(chunk->data);
        free(chunk);
    }

    printf("Cons_%x : %ld chunks, %ld bytes\n",
           consumer->id,
           consumer->handledChunks,
           consumer->handledBytes);

    pthread_exit(consumer);
}
