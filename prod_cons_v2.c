#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <time.h>
#include <errno.h>

#ifndef QUEUE_CAP
    #define QUEUE_CAP 1024 // Number of lines that can be buffered
#endif

#ifndef MAX_CONSUMERS
    #define MAX_CONSUMERS 256
#endif

static double now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1000.0 + (double)ts.tv_nsec / 1e6;
}

// linked-list based bounded queue for lines, circular buffer implementation
typedef struct {
    char   **buf;
    size_t   cap;
    size_t   head;
    size_t   tail;
    size_t   count;

    int      done; // producer has finished pushing all lines

    pthread_mutex_t m;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;
} line_queue_t;

static void q_init(line_queue_t *q, size_t cap) {
    q->buf = (char**)calloc(cap, sizeof(char*));
    if (!q->buf) { perror("calloc queue"); exit(1); }
    q->cap = cap;
    q->head = q->tail = q->count = 0;
    q->done = 0;
    pthread_mutex_init(&q->m, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
}

static void q_destroy(line_queue_t *q) {
    // Any remaining items should be freed by caller before destroy if needed
    free(q->buf);
    pthread_mutex_destroy(&q->m);
    pthread_cond_destroy(&q->not_empty);
    pthread_cond_destroy(&q->not_full);
}

static void q_push(line_queue_t *q, char *line) {
    pthread_mutex_lock(&q->m);
    while (q->count == q->cap) {
        pthread_cond_wait(&q->not_full, &q->m);
    }
    q->buf[q->tail] = line;
    q->tail = (q->tail + 1) % q->cap;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->m);
}

// Returns 1 if a line was popped;
// 0 if queue is empty and producer done
static int q_pop(line_queue_t *q, char **out_line) {
    pthread_mutex_lock(&q->m);
    while (q->count == 0 && !q->done) {
        pthread_cond_wait(&q->not_empty, &q->m);
    }
    if (q->count == 0 && q->done) {
        // no more work
        pthread_mutex_unlock(&q->m);
        return 0;
    }
    *out_line = q->buf[q->head];
    q->buf[q->head] = NULL;
    q->head = (q->head + 1) % q->cap;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->m);
    return 1;
}

// Shared state
typedef struct {
    FILE        *rfile;
    line_queue_t queue;

    // Final global A..Z counters (merged at the end)
    unsigned long global_alpha[26];
    pthread_mutex_t global_lock;

    // Optional printing control
    int print_lines;
} shared_t;

// Producer
typedef struct {
    shared_t *S;
    long      produced;
} prod_arg_t;

static void *producer_main(void *argp) {
    prod_arg_t *arg = (prod_arg_t*)argp;
    shared_t *S = arg->S;

    char *line = NULL;
    size_t len = 0;
    ssize_t nread;
    long produced = 0;

    while ((nread = getdelim(&line, &len, '\n', S->rfile)) != -1) {
        // Duplicate the line so queue owns it
        char *copy = (char*)malloc((size_t)nread + 1);
        if (!copy) {
            perror("malloc");
            exit(1);
        }
        memcpy(copy, line, (size_t)nread);
        copy[nread] = '\0';
        q_push(&S->queue, copy);
        produced++;
    }
    free(line);

    // Signal completion
    pthread_mutex_lock(&S->queue.m);
    S->queue.done = 1;
    pthread_cond_broadcast(&S->queue.not_empty);
    pthread_mutex_unlock(&S->queue.m);

    arg->produced = produced;
    return NULL;
}

// Consumer
typedef struct {
    shared_t *S;
    int       id;
    long      consumed;
    unsigned long local_alpha[26]; // per-thread counts to reduce contention
} cons_arg_t;

static void count_alphabet(const char *s, unsigned long alpha[26]) {
    for (const unsigned char *p = (const unsigned char*)s; *p; ++p) {
        if (isalpha(*p)) {
            int idx = tolower(*p) - 'a';
            if (0 <= idx && idx < 26)
                alpha[idx]++;
        }
    }
}

static void *consumer_main(void *argp) {
    cons_arg_t *arg = (cons_arg_t*)argp;
    shared_t *S = arg->S;
    memset(arg->local_alpha, 0, sizeof(arg->local_alpha));

    char *line = NULL;
    long consumed = 0;
    while (q_pop(&S->queue, &line)) {
        if (S->print_lines) {
            // include thread id and index to mimic original sample style
            printf("Cons_%02d: %s", arg->id, line);
            if (line[0] && line[strlen(line)-1] != '\n') printf("\n");
        }
        count_alphabet(line, arg->local_alpha);
        free(line);
        line = NULL;
        consumed++;
    }

    // Merge local counter into global once per consumer
    pthread_mutex_lock(&S->global_lock);
    for (int i = 0; i < 26; ++i) {
        S->global_alpha[i] += arg->local_alpha[i];
    }
    pthread_mutex_unlock(&S->global_lock);

    arg->consumed = consumed;
    return NULL;
}

static void print_alpha_stats(const unsigned long alpha[26]) {
    printf("\n=== Alphabet frequency (A..Z, case-insensitive) ===\n");
    for (int i = 0; i < 26; ++i) {
        printf("%c: %lu\n", 'A' + i, alpha[i]);
    }

    // Optional: one-row summary like char_stat.c
    printf("\nSummary row (A..Z):\n");
    for (int i = 0; i < 26; ++i) {
        printf("%8lu", alpha[i]);
    }
    printf("\n");
}

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "usage: %s <readfile> <#producers> <#consumers>\n", argv[0]);
        return 1;
    }

    const char *path = argv[1];
    int nprod = atoi(argv[2]);
    int ncons = atoi(argv[3]);
    int print_lines = 1;

    if (nprod < 1) 
        nprod = 1;
    if (ncons < 1) 
        ncons = 1;

    FILE *rf = fopen(path, "rb");
    if (!rf) {
        perror(path);
        return 1;
    }

    shared_t S;
    S.rfile = rf;
    q_init(&S.queue, QUEUE_CAP);
    memset(S.global_alpha, 0, sizeof(S.global_alpha));
    pthread_mutex_init(&S.global_lock, NULL);
    S.print_lines = print_lines;

    // Producer(s): we support only one; ignore extras gracefully
    prod_arg_t parg = { .S = &S, .produced = 0 };
    pthread_t prod_th;
    double t0 = now_ms();
    if (pthread_create(&prod_th, NULL, producer_main, &parg) != 0) {
        perror("pthread_create producer");
        return 1;
    }

    // Consumers
    cons_arg_t *cargs = (cons_arg_t*)calloc((size_t)ncons, sizeof(cons_arg_t));
    pthread_t  *cth   = (pthread_t*)calloc((size_t)ncons, sizeof(pthread_t));
    if (!cargs || !cth) {
        perror("calloc");
        return 1;
    }

    for (int i = 0; i < ncons; ++i) {
        cargs[i].S = &S;
        cargs[i].id = i;
        cargs[i].consumed = 0;
        if (pthread_create(&cth[i], NULL, consumer_main, &cargs[i]) != 0) {
            perror("pthread_create consumer");
            return 1;
        }
    }

    pthread_join(prod_th, NULL);
    for (int i = 0; i < ncons; ++i) pthread_join(cth[i], NULL);
    double t1 = now_ms();

    printf("\n--- Run summary ---\n");
    printf("Produced lines: %ld\n", parg.produced);
    long total_consumed = 0;
    for (int i = 0; i < ncons; ++i) total_consumed += cargs[i].consumed;
    printf("Consumed lines: %ld (across %d consumers)\n", total_consumed, ncons);
    printf("Elapsed time: %.3f ms\n", t1 - t0);

    print_alpha_stats(S.global_alpha);

    fclose(rf);
    q_destroy(&S.queue);
    pthread_mutex_destroy(&S.global_lock);
    free(cargs);
    free(cth);

    return 0;
}
