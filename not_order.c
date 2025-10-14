
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <time.h>

#define BUF_SIZE    100     // stack capacity (number of batches)
#define BATCH_LINES 4400    // lines per batch (tune as needed)

int stat[26];
pthread_mutex_t stat_mtx = PTHREAD_MUTEX_INITIALIZER;

/* ------------ stats: local -> merge ------------ */
static void count_alpha_local(const char *s, int local[26]) {
    for (int i = 0; i < 26; ++i) local[i] = 0;
    for (const unsigned char *p = (const unsigned char*)s; *p; ++p) {
        unsigned char c = *p;
        if (c >= 'A' && c <= 'Z') local[c - 'A']++;
        else if (c >= 'a' && c <= 'z') local[c - 'a']++;
    }
}
static void merge_counts(const int local[26]) {
    pthread_mutex_lock(&stat_mtx);
    for (int i = 0; i < 26; ++i) stat[i] += local[i];
    pthread_mutex_unlock(&stat_mtx);
}
static void print_counts(void) {
    for (int i = 0; i < 26; ++i) {
        if (stat[i] > 0) printf("%c: %d\n", 'a' + i, stat[i]);
    }
}

/* ------------ shared object ------------ */
typedef struct sharedobject {
    FILE *rfile;

    // protect stdio file access among producers
    pthread_mutex_t file_mtx;

    // simple stack of batches (unordered output)
    char *line[BUF_SIZE];
    int   count;          // number of items in stack (0..BUF_SIZE)

    // stack synchronization
    pthread_mutex_t lock;
    pthread_cond_t  not_empty;
    pthread_cond_t  not_full;

    // termination
    int end;              // no more pushes (all producers finished)
    int nprod;            // total producers
    int done_prod;        // number of finished producers
} so_t;

/* ------------ producer ------------ */
static void *producer(void *arg) {
    so_t *so = (so_t*)arg;
    int *ret = (int*)malloc(sizeof(int));
    if (!ret) pthread_exit(NULL);
    *ret = 0;

    char  *line = NULL;     // getdelim buffer
    size_t llen  = 0;       // getdelim buffer cap
    ssize_t nread;

    for (;;) {
        char  *batch = NULL;  // concatenated lines buffer
        size_t bcap  = 0, blen = 0;
        int    got   = 0;

        // ---- read up to BATCH_LINES under file_mtx (batch-level atomic read) ----
        pthread_mutex_lock(&so->file_mtx);
        for (int k = 0; k < BATCH_LINES; ++k) {
            nread = getdelim(&line, &llen, '\n', so->rfile);
            if (nread == -1) break;

            if (blen + (size_t)nread + 1 > bcap) {
                size_t nb = (blen + (size_t)nread + 1) * 2;
                char *tmp = (char*)realloc(batch, nb);
                if (!tmp) {
                    // release file lock before exiting thread
                    pthread_mutex_unlock(&so->file_mtx);
                    free(batch);
                    free(line);
                    perror("realloc");
                    pthread_exit(NULL);
                }
                batch = tmp; bcap = nb;
            }
            memcpy(batch + blen, line, (size_t)nread);
            blen += (size_t)nread;
            batch[blen] = '\0';
            got++;
        }
        pthread_mutex_unlock(&so->file_mtx);

        if (got == 0) {
            // this producer reached EOF immediately
            pthread_mutex_lock(&so->lock);
            so->done_prod++;
            if (so->done_prod == so->nprod) {
                // last producer finishes -> signal consumers to drain/exit
                so->end = 1;
                pthread_cond_broadcast(&so->not_empty);
            }
            pthread_mutex_unlock(&so->lock);
            free(batch);
            break;
        }

        // ---- push the batch onto the stack ----
        pthread_mutex_lock(&so->lock);
        while (so->count == BUF_SIZE) {
            pthread_cond_wait(&so->not_full, &so->lock);
        }
        so->line[so->count] = batch;  // LIFO push
        so->count++;
        pthread_cond_signal(&so->not_empty);
        pthread_mutex_unlock(&so->lock);

        *ret += got;
    }

    free(line);
    printf("Prod_%x: %d lines\n", (unsigned int)pthread_self(), *ret);
    pthread_exit(ret);
}

/* ------------ consumer ------------ */
static void *consumer(void *arg) {
    so_t *so = (so_t*)arg;
    int *ret = (int*)malloc(sizeof(int));
    if (!ret) pthread_exit(NULL);
    *ret = 0;

    for (;;) {
        pthread_mutex_lock(&so->lock);
        while (so->count == 0 && !so->end) {
            pthread_cond_wait(&so->not_empty, &so->lock);
        }
        if (so->count == 0 && so->end) {
            pthread_mutex_unlock(&so->lock);
            break; // nothing left and no more will arrive
        }

        // pop from stack
        so->count--;
        char *batch = so->line[so->count];
        pthread_cond_signal(&so->not_full);
        pthread_mutex_unlock(&so->lock);

        // count alphabet stats for this batch without holding the queue lock
        int local[26];
        count_alpha_local(batch, local);
        merge_counts(local);

        free(batch);
        (*ret)++;
    }

    printf("Cons_%x: %d batches\n", (unsigned int)pthread_self(), *ret);
    pthread_exit(ret);
}

/* ------------ main ------------ */
int main(int argc, char *argv[]) {
    clock_t c0 = clock();

    if (argc < 2) {
        printf("usage: %s <readfile> [#Producer] [#Consumer]\n", argv[0]);
        return 0;
    }

    // init global stats
    memset(stat, 0, sizeof(stat));

    FILE *rfile = fopen(argv[1], "r");
    if (!rfile) { perror("rfile"); return 1; }

    int Nprod = (argc >= 3) ? atoi(argv[2]) : 1;
    int Ncons = (argc >= 4) ? atoi(argv[3]) : 1;
    if (Nprod < 1) Nprod = 1; if (Nprod > 100) Nprod = 100;
    if (Ncons < 1) Ncons = 1; if (Ncons > 100) Ncons = 100;

    so_t *share = (so_t*)calloc(1, sizeof(so_t));
    if (!share) { perror("calloc"); fclose(rfile); return 1; }

    share->rfile = rfile;
    share->count = 0;
    share->end   = 0;
    share->nprod = Nprod;
    share->done_prod = 0;

    pthread_mutex_init(&share->file_mtx, NULL);
    pthread_mutex_init(&share->lock, NULL);
    pthread_cond_init(&share->not_empty, NULL);
    pthread_cond_init(&share->not_full, NULL);

    pthread_t prod[100], cons[100];

    for (int i = 0; i < Nprod; ++i)
        pthread_create(&prod[i], NULL, producer, share);
    for (int i = 0; i < Ncons; ++i)
        pthread_create(&cons[i], NULL, consumer, share);

    int total_cons_batches = 0, total_prod_lines = 0;
    int *ret;

    for (int i = 0; i < Ncons; ++i) {
        if (pthread_join(cons[i], (void**)&ret) == 0 && ret) {
            total_cons_batches += *ret;
            printf("main: consumer_%d joined with %d batches\n", i, *ret);
            free(ret);
        }
    }
    for (int i = 0; i < Nprod; ++i) {
        if (pthread_join(prod[i], (void**)&ret) == 0 && ret) {
            total_prod_lines += *ret;
            printf("main: producer_%d joined with %d lines\n", i, *ret);
            free(ret);
        }
    }

    clock_t c1 = clock();
    printf("CPU time: %.3f ms\n", 1000.0 * (c1 - c0) / CLOCKS_PER_SEC);

    // drain any leftover (should be none)
    pthread_mutex_lock(&share->lock);
    while (share->count > 0) free(share->line[--share->count]);
    pthread_mutex_unlock(&share->lock);

    pthread_cond_destroy(&share->not_empty);
    pthread_cond_destroy(&share->not_full);
    pthread_mutex_destroy(&share->lock);
    pthread_mutex_destroy(&share->file_mtx);
    pthread_mutex_destroy(&stat_mtx);
    fclose(rfile);
    free(share);

    // final stats
    print_counts();
    return 0;
}
