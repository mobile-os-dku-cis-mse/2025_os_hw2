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
static void cleanup_fclose(void *p) {
    FILE *f = (FILE*)p;
    if (f) fclose(f);
}

typedef struct sharedobject {
    FILE *rfile;
    int linenum;
    char *line;
    int full;
    pthread_mutex_t lock;
    pthread_cond_t  cv;
} so_t;

void *producer(void *arg) {
    so_t *so = arg;
    FILE *rfile = so->rfile;

    char *line = NULL;
    size_t len = 0;
    ssize_t read = 0;
    int cnt = 0;

    pthread_cleanup_push(cleanup_free_line, &line);
    pthread_cleanup_push(cleanup_fclose, rfile);

    while (1) {
        read = getdelim(&line, &len, '\n', rfile);
        // wait until consumer signals
        pthread_mutex_lock(&so->lock);
        while (so->full == 1) {
            pthread_cond_wait(&so->cv, &so->lock);
        }

        if (read == -1) {
            so->line = NULL;
            so->full = 1;
            pthread_cond_signal(&so->cv);
            pthread_mutex_unlock(&so->lock);
            break;
        }

        so->linenum = (int)cnt;
        so->line = strdup(line);
        so->full = 1;
        pthread_cond_signal(&so->cv);
        pthread_mutex_unlock(&so->lock);

        printf("Producer: [%02d:%02d] %s", cnt, so->linenum, so->line);
        cnt++;
    }

    pthread_cleanup_pop(1); // fclose(rfile)
    pthread_cleanup_pop(1); // free(line)

    THREAD_RETURN_INT(cnt);
    return NULL;
}

void *consumer(void *arg) {
    so_t *so = arg;
    int cnt = 0;

    while (1) {
        pthread_mutex_lock(&so->lock);
        while (so->full == 0) {
            pthread_cond_wait(&so->cv, &so->lock);
        }

        if (so->line == NULL) {
            so->full = 0;
            pthread_cond_signal(&so->cv);
            pthread_mutex_unlock(&so->lock);
            break;
        }

        printf("Consumer: [%02d:%02d] %s", cnt, so->linenum, so->line);
        free(so->line);

        so->full = 0;
        pthread_cond_signal(&so->cv);
        pthread_mutex_unlock(&so->lock);

        cnt++;
    }

    THREAD_RETURN_INT(cnt);
    return NULL;
}

int run_prod_cons(int argc, char *argv[]) {
    pthread_t prod[100];
    pthread_t cons[100];
    int Nprod, Ncons;
    FILE *rfile;

    if (argc < 2) {
        fprintf(stderr, "usage: %s <readfile>\n", argv[0]);
        exit(1);
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

    rfile = fopen(argv[1], "r");
    if (!rfile) {
        perror("fopen");
        exit(1);
    }

    so_t *share = calloc(1, sizeof(so_t));
    if (!share) {
        perror("calloc");
        fclose(rfile);
        return 1;
    }

    share->rfile = rfile;
    share->line = NULL;
    share->full = 0;
    pthread_mutex_init(&share->lock, NULL);
    pthread_cond_init(&share->cv, NULL);

    for (int i = 0; i < Nprod; i++)
         pthread_create(&prod[i], NULL, producer, share);
    for (int i = 0; i < Ncons; i++)
        pthread_create(&cons[i], NULL, consumer, share);

    printf("main continuing\n");

    long csum = 0;
    for (int i = 0; i < Ncons; i++) {
        long n = -1;
        THREAD_JOIN_INT(cons[i], n);
        printf("main: consumer_%d joined with %ld\n", i, n);
        if (n > 0) csum += n;
    }
    for (int i = 0; i < Nprod; i++) {
        long n = -1;
        THREAD_JOIN_INT(prod[i], n);
        printf("main: producer_%d joined with %ld\n", i, n);
    }

    pthread_mutex_destroy(&share->lock);
    pthread_cond_destroy(&share->cv);
    free(share);

    return 0;
}