#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>


#define NUM_THREADS 4
#define BUFFER_SIZE (1024 * 1024)

typedef struct {
    int fd;
    off_t start_offset;
    off_t end_offset;
    int chunk;
    long long* result_bytes;
} FileIndicator;

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


int main(int argc, char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <filename>\n", argv[0]);
        return 1;
    }
    const char* filename = argv[1];

    int fd = open(filename, O_RDONLY);
    if (fd == -1) {
        perror("Failed to open file");
        return 1;
    }

    struct stat file_stat;
    if (fstat(fd, &file_stat) == -1) {
        perror("Failed to get file stats");
        close(fd);
        return 1;
    }
    off_t total_file_size = file_stat.st_size;
    printf("Total file size: %lld bytes\n", (long long)total_file_size);

    if (total_file_size == 0) {
        printf("File is empty.\n");
        close(fd);
        return 0;
    }

    pthread_t threads[NUM_THREADS];
    FileIndicator thread_args[NUM_THREADS];
    long long thread_results[NUM_THREADS] = {0};

    off_t chunk_size_per_thread = total_file_size / NUM_THREADS;
    off_t current_offset = 0;

    printf("Assigning work to %d threads...\n", NUM_THREADS);

    for (int i = 0; i < NUM_THREADS; ++i) {
        thread_args[i].thread_id = i;
        thread_args[i].fd = fd;
        thread_args[i].start_offset = current_offset;
        thread_args[i].result_bytes = &thread_results[i];

        if (i == NUM_THREADS - 1) {
            thread_args[i].end_offset = total_file_size;
        } else {
            thread_args[i].end_offset = current_offset + chunk_size_per_thread;
        }

        printf("  Thread %d: %lld (start) -> %lld (end) [Size: %lld bytes]\n",
               i, 
               (long long)thread_args[i].start_offset, 
               (long long)thread_args[i].end_offset,
               (long long)(thread_args[i].end_offset - thread_args[i].start_offset));

        current_offset += chunk_size_per_thread;
    }

    for (int i = 0; i < NUM_THREADS; ++i) {
        if (pthread_create(&threads[i], NULL, read_file_chunk, &thread_args[i]) != 0) {
            perror("Failed to create thread");
            close(fd);
            return 1;
        }
    }

    long long total_bytes_read_from_threads = 0;
    for (int i = 0; i < NUM_THREADS; ++i) {
        pthread_join(threads[i], NULL);
        printf("Thread %d result: Read %lld bytes.\n", i, thread_results[i]);
        total_bytes_read_from_threads += thread_results[i];
    }

    printf("\n--- Summary ---\n");
    printf("Original file size: %lld\n", (long long)total_file_size);
    printf("Total bytes read by threads: %lld\n", total_bytes_read_from_threads);

    if (total_bytes_read_from_threads == total_file_size) {
        printf("Success: All file bytes read correctly.\n");
    } else {
        printf("Error: Mismatch in bytes read.\n");
    }

    close(fd);
    return 0;
}