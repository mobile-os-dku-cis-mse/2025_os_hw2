#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>

static int g_quiet = 0;

#define THREAD_RETURN_INT(v) \
    do { pthread_exit((void*)(intptr_t)(v)); } while (0)

#define THREAD_JOIN_INT(tid, out_long_var)                  \
    do {                                                    \
        void *___retp = NULL;                               \
        int ___rc = pthread_join((tid), &___retp);          \
        if (___rc != 0) {                                   \
            (out_long_var) = -1;                            \
        } else {                                            \
            (out_long_var) = (long)(intptr_t)___retp;       \
        }                                                   \
    } while (0)

typedef struct {
    int id;
    char *line;
} item_t;

typedef struct {
    // Queue
    item_t *buf;
    int cap;
    int head;
    int tail;
    int count;

    // Synchronization
    pthread_mutex_t qlock;         // protects queue
    pthread_cond_t notFull;
    pthread_cond_t notEmpty;

    // File (shared read)
    FILE *fp;
    pthread_mutex_t flock;         // protects file access

    // Termination
    int eof;
} so_t;

/* ---------- Producer ---------- */

void *producer(void *arg) {
    so_t *so = (so_t*)arg;
    char *line = NULL;
    size_t len = 0;
    ssize_t nread;
    long produced = 0;

    for (;;) {
        pthread_mutex_lock(&so->flock);
        nread = getdelim(&line, &len, '\n', so->fp);
        pthread_mutex_unlock(&so->flock);
        if (nread == -1) {
            // wake up all consumers in case they are sleeping
            pthread_mutex_lock(&so->qlock);
            if (!so->eof) {
                so->eof = 1;
                pthread_cond_broadcast(&so->notEmpty);
            }
            pthread_mutex_unlock(&so->qlock);
            break;
        }

        char *copy = strdup(line);
        int id = (int)produced;

        // enqueue (bounded buffer)
        pthread_mutex_lock(&so->qlock);
        while (so->count == so->cap) {
            pthread_cond_wait(&so->notFull, &so->qlock);
        }

        int pos = so->tail;
        so->buf[pos].id = id;
        so->buf[pos].line = copy;
        so->tail = (so->tail + 1) % so->cap;
        so->count++;

        pthread_cond_signal(&so->notEmpty);
        pthread_mutex_unlock(&so->qlock);
        produced++;
    }

    free(line);
    THREAD_RETURN_INT(produced);
}

/* ---------- Consumer ---------- */

void *consumer(void *arg) {
    so_t *so = (so_t*)arg;
    long consumed = 0;

    for (;;) {
        pthread_mutex_lock(&so->qlock);
        while (so->count == 0 && !so->eof) {
            pthread_cond_wait(&so->notEmpty, &so->qlock);
        }

        if (so->count == 0 && so->eof) {
            pthread_mutex_unlock(&so->qlock);
            break;
        }

        // Dequeue
        int pos = so->head;
        item_t item = so->buf[pos];
        so->head = (so->head + 1) % so->cap;
        so->count--;

        pthread_cond_signal(&so->notFull);
        pthread_mutex_unlock(&so->qlock);

        // Process item outside lock
        if (!g_quiet) {
            printf("Cons_%lx: [%02ld:%02d] %s",
                   (unsigned long)pthread_self(), consumed, item.id, item.line);
        }
        free(item.line);
        consumed++;
    }

    if (!g_quiet) {
        printf("\nCons_%lx: consumed %ld lines\n",
               (unsigned long)pthread_self(), consumed);
    }
    THREAD_RETURN_INT(consumed);
}

/* ---------- Driver ---------- */

int run_prod_cons_v2(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <file> [nprod] [ncons] [qcap]\n", argv[0]);
        return 1;
    }

    /* Quiet mode: 환경변수 PC_QUIET=1이면 printf 억제 */
    const char* qenv = getenv("PC_QUIET");
    if (qenv && qenv[0] == '1') g_quiet = 1;

    int nprod = (argc > 2) ? atoi(argv[2]) : 1;
    int ncons = (argc > 3) ? atoi(argv[3]) : 1;
    int qcap  = (argc > 4) ? atoi(argv[4]) : 8;

    if (nprod < 1) nprod = 1;
    if (ncons < 1) ncons = 1;
    if (qcap  < 1) qcap  = 4;

    FILE *fp = fopen(argv[1], "r");
    if (!fp) { perror("fopen"); return 1; }

    so_t so = {
        .buf = (item_t*)calloc(qcap, sizeof(item_t)),
        .cap = qcap,
        .head = 0,
        .tail = 0,
        .count = 0,
        .fp = fp,
        .eof = 0
    };

    pthread_mutex_init(&so.qlock, NULL);
    pthread_mutex_init(&so.flock, NULL);
    pthread_cond_init(&so.notFull, NULL);
    pthread_cond_init(&so.notEmpty, NULL);

    pthread_t prod[nprod], cons[ncons];
    for (int i = 0; i < nprod; i++)
        pthread_create(&prod[i], NULL, producer, &so);
    for (int i = 0; i < ncons; i++)
        pthread_create(&cons[i], NULL, consumer, &so);

    if (!g_quiet) printf("main continuing...\n");

    long pcount = 0, ccount = 0;
    for (int i = 0; i < nprod; i++) {
        long n = -1;
        THREAD_JOIN_INT(prod[i], n);
        pcount += n;
    }
    for (int i = 0; i < ncons; i++) {
        long n = -1;
        THREAD_JOIN_INT(cons[i], n);
        ccount += n;
    }

    if (!g_quiet) {
        printf("\nDone: produced=%ld, consumed=%ld\n", pcount, ccount);
    }

    pthread_mutex_destroy(&so.qlock);
    pthread_mutex_destroy(&so.flock);
    pthread_cond_destroy(&so.notFull);
    pthread_cond_destroy(&so.notEmpty);

    free(so.buf);
    fclose(fp);
    return 0;
}

