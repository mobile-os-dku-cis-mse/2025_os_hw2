#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <stdint.h>

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

static void cleanup_free_line(void *p) {
    char **pl = (char**)p;     // p == &line
    if (pl && *pl) { free(*pl); *pl = NULL; }
}

typedef struct sharedobject {
    FILE *rfile;
    int linenum;
    char *line;
    int full;
    int eof; 
    pthread_cond_t  cv;
    pthread_mutex_t lock;
} so_t;

void *producer(void *arg) {
    so_t *so = (so_t*)arg;
    FILE *rfile = so->rfile;

    char *line = NULL;
    size_t len = 0;         
    ssize_t read = 0;
    int cnt = 0;

    pthread_cleanup_push(cleanup_free_line, &line);

    while (1) {
        pthread_mutex_lock(&so->lock);
        while (so->full) {
            pthread_cond_wait(&so->cv, &so->lock);
        }

        read = getdelim(&line, &len, '\n', rfile);
        if (read == -1) {
            so->eof = 1;
            pthread_cond_broadcast(&so->cv);
            pthread_mutex_unlock(&so->lock);
            break;
        }

        so->linenum = (int)cnt;
        so->line = line;
        so->full = 1;
        int pnum = so->linenum;
        char *pline = strdup(so->line);
        pthread_cond_signal(&so->cv);
        pthread_mutex_unlock(&so->lock);

        printf("Prod: [%02d:%02d] %s", cnt, pnum, pline);
        free(pline);
        cnt++;
    }

    pthread_cleanup_pop(1); 
    THREAD_RETURN_INT(cnt);
    return NULL;
}

void *consumer(void *arg) {
    so_t *so = (so_t*)arg;
    int cnt = 0;

    while (1) {
        pthread_mutex_lock(&so->lock);
        while (!so->full && !so->eof) {
            pthread_cond_wait(&so->cv, &so->lock);
        }

        if (!so->full && so->eof) {
            pthread_mutex_unlock(&so->lock);
            break;
        }

        if (so->line == NULL) {
            so->full = 0;
            pthread_cond_broadcast(&so->cv);
            pthread_mutex_unlock(&so->lock);
            break;
        }

        char *line_copy = strdup(so->line);
        int num = so->linenum;
        so->full = 0;
        pthread_cond_signal(&so->cv);
        pthread_mutex_unlock(&so->lock);

        printf("Cons_%x: [%02d:%02d] %s", (unsigned int)pthread_self(), cnt, num, line_copy);
        free(line_copy);
        cnt++;
    }

    printf("\nCons: %d lines", cnt);
    THREAD_RETURN_INT(cnt);
    return NULL;
}

int run_prod_cons(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <readfile>\n", argv[0]);
        exit(1);
    }
    int Nprod = (argc > 2) ? atoi(argv[2]) : 1;
    int Ncons = (argc > 3) ? atoi(argv[3]) : 1;
    if (Nprod < 1) Nprod = 1;
    if (Ncons < 1) Ncons = 1;

    FILE *f = fopen(argv[1], "r");
    if (!f) { perror("fopen"); return 1; }

    so_t share = {
        .rfile = f,
        .linenum = 0,
        .line = NULL,
        .full = 0,
        .eof = 0
    };
    pthread_mutex_init(&share.lock, NULL);
    pthread_cond_init(&share.cv, NULL);

    pthread_t prod[Nprod], cons[Ncons];

    for (int i = 0; i < Nprod; i++)
         pthread_create(&prod[i], NULL, producer, &share);
    for (int i = 0; i < Ncons; i++)
        pthread_create(&cons[i], NULL, consumer, &share);

    printf("main continuing...\n");

    long pcount = 0, ccount = 0;
    for (int i = 0; i < Nprod; i++) {
        long n = -1;
        THREAD_JOIN_INT(prod[i], n);
        pcount += n;
    }
    for (int i = 0; i < Ncons; i++) {
        long n = -1;
        THREAD_JOIN_INT(cons[i], n);
        ccount += n;
    }
    printf("\nDone: produced=%ld, consumed=%ld\n", pcount, ccount);

    pthread_mutex_destroy(&share.lock);
    pthread_cond_destroy(&share.cv);
    if (share.line) free(share.line);
    fclose(f);
    return 0;
}