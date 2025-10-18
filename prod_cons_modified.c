#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <sys/time.h>

#define BUFFER_SIZE 10 // Circular buffer size
#define ALPHABET 26    // For character statistics

typedef struct {
    char *lines[BUFFER_SIZE]; // Circular buffer for lines
    int head, tail, count;    // Buffer indices and count
    FILE *rfile;              // File pointer
    int done;                 // Flag when all producers finish
    int producers_running;    // Number of producers still running
    pthread_mutex_t lock;     // Mutex for synchronization
    pthread_cond_t not_empty; // Condition: buffer not empty
    pthread_cond_t not_full;  // Condition: buffer not full
} shared_t;

// Global statistics array
int global_stats[ALPHABET];
pthread_mutex_t stats_lock = PTHREAD_MUTEX_INITIALIZER;

// Producer thread: reads lines and puts them into buffer
void *producer(void *arg) {
    shared_t *so = (shared_t *)arg;
    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    int produced = 0;

    while (1) {
        // Read from file (no lock needed, only one producer should read)
        pthread_mutex_lock(&so->lock);
        read = getdelim(&line, &len, '\n', so->rfile);
        pthread_mutex_unlock(&so->lock);

        if (read == -1) break;

        // Now lock buffer and wait if full
        pthread_mutex_lock(&so->lock);
        while (so->count == BUFFER_SIZE) {
            pthread_cond_wait(&so->not_full, &so->lock);
        }
        so->lines[so->tail] = strdup(line);
        so->tail = (so->tail + 1) % BUFFER_SIZE;
        so->count++;
        pthread_cond_signal(&so->not_empty);
        pthread_mutex_unlock(&so->lock);
        produced++;
    }

    free(line);
    pthread_mutex_lock(&so->lock);
    so->producers_running--;
    if (so->producers_running == 0) {
        so->done = 1;
        pthread_cond_broadcast(&so->not_empty);
    }
    pthread_mutex_unlock(&so->lock);

    printf("Producer %lu: %d lines produced\n", pthread_self(), produced);
    int *ret = malloc(sizeof(int));
    *ret = produced;
    pthread_exit(ret);
}

// Consumer thread: takes lines from buffer and processes them
void *consumer(void *arg) {
    shared_t *so = (shared_t *)arg;
    int consumed = 0;
    int local_stats[ALPHABET] = {0};

    while (1) {
        pthread_mutex_lock(&so->lock);
        while (so->count == 0 && !so->done) {
            pthread_cond_wait(&so->not_empty, &so->lock);
        }
        if (so->count == 0 && so->done) {
            pthread_mutex_unlock(&so->lock);
            break;
        }
        char *line = so->lines[so->head];
        so->head = (so->head + 1) % BUFFER_SIZE;
        so->count--;
        pthread_cond_signal(&so->not_full);
        pthread_mutex_unlock(&so->lock);

        // Process line: print and update local stats
        //printf("Consumer %lu: %s", pthread_self(), line);
        for (int i = 0; line[i]; i++) {
            if (isalpha((unsigned char)line[i])) {
                local_stats[tolower(line[i]) - 'a']++;
            }
        }
        free(line);
        consumed++;
    }

    // Merge local stats into global stats
    pthread_mutex_lock(&stats_lock);
    for (int i = 0; i < ALPHABET; i++) {
        global_stats[i] += local_stats[i];
    }
    pthread_mutex_unlock(&stats_lock);

    //printf("Consumer %lu: %d lines consumed\n", pthread_self(), consumed);
    int *ret = malloc(sizeof(int));
    *ret = consumed;
    pthread_exit(ret);
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        printf("Usage: %s <filename> <#producers> <#consumers>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int Nprod = atoi(argv[2]);
    int Ncons = atoi(argv[3]);
    if (Nprod <= 0) Nprod = 1;
    if (Ncons <= 0) Ncons = 1;

    FILE *rfile = fopen(argv[1], "r");
    if (!rfile) {
        perror("File open error");
        exit(EXIT_FAILURE);
    }

    printf("File opened: %s\n", argv[1]);

    shared_t so;
    memset(&so, 0, sizeof(shared_t));
    so.rfile = rfile;
    so.producers_running = Nprod; // Track running producers
    pthread_mutex_init(&so.lock, NULL);
    pthread_cond_init(&so.not_empty, NULL);
    pthread_cond_init(&so.not_full, NULL);

    pthread_t prod_threads[Nprod], cons_threads[Ncons];

    struct timeval start, end;
    gettimeofday(&start, NULL);

    for (int i = 0; i < Nprod; i++) {
        pthread_create(&prod_threads[i], NULL, producer, &so);
    }
    for (int i = 0; i < Ncons; i++) {
        pthread_create(&cons_threads[i], NULL, consumer, &so);
    }

    int *ret;
    for (int i = 0; i < Nprod; i++) {
        pthread_join(prod_threads[i], (void **)&ret);
        printf("Main: Producer %d joined with %d\n", i, *ret);
        free(ret);
    }
    for (int i = 0; i < Ncons; i++) {
        pthread_join(cons_threads[i], (void **)&ret);
        printf("Main: Consumer %d joined with %d\n", i, *ret);
        free(ret);
    }

    gettimeofday(&end, NULL);
    double elapsed = (end.tv_sec - start.tv_sec) + (end.tv_usec - start.tv_usec)/1e6;
    printf("Execution time: %.4f seconds\n", elapsed);

    // Print global statistics
    printf("\nCharacter Statistics:\n");
    for (int i = 0; i < ALPHABET; i++) {
        printf("%c: %d\n", 'a' + i, global_stats[i]);
    }

    fclose(rfile);
    return 0;
}