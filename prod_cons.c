#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

typedef struct sharedobject {
    FILE *rfile;
    int linenum;
    char *line;
    pthread_mutex_t lock;
    pthread_cond_t cond;        
    pthread_cond_t cond_empty;  
    int full;
} so_t;

void *producer(void *arg) {
    so_t *so = arg;
    int *ret = malloc(sizeof(int));
    if (!ret) pthread_exit(NULL);

    FILE *rfile = so->rfile;
    int i = 0;
    char *line = NULL;
    size_t len = 0;
    ssize_t read = 0;

    while (1) {
        pthread_mutex_lock(&so->lock);
        while (so->full) {
            pthread_cond_wait(&so->cond_empty, &so->lock);
        }
        read = getdelim(&line, &len, '\n', rfile);
        if (read == -1) {
            so->full = 1;
            so->line = NULL; 
            pthread_cond_broadcast(&so->cond);
            pthread_mutex_unlock(&so->lock);
            break;
        }

        so->linenum = i;
        so->line = strdup(line);      
        i++;
        so->full = 1;

        pthread_cond_signal(&so->cond);
        pthread_mutex_unlock(&so->lock);
    }

    free(line);
    printf("Prod_%lu: %d lines\n", (unsigned long)pthread_self(), i);
    *ret = i;
    pthread_exit(ret);
}

void *consumer(void *arg) {
    so_t *so = arg;
    int *ret = malloc(sizeof(int));
    if (!ret) pthread_exit(NULL);

    int i = 0;
    int len;
    char *line;

    while (1) {
        pthread_mutex_lock(&so->lock);
        while (!so->full) {
            pthread_cond_wait(&so->cond, &so->lock);
        }

        if (so->line == NULL) {
            pthread_cond_broadcast(&so->cond);
            pthread_mutex_unlock(&so->lock);
            break;
        }

        line = so->line;
        so->line = NULL;
        len = strlen(line);
        printf("Cons_%lu: [%02d:%02d] %s",
               (unsigned long)pthread_self(), i, so->linenum, line);
        free(line);

        i++;
        so->full = 0;

        pthread_cond_signal(&so->cond_empty);
        pthread_mutex_unlock(&so->lock);
    }

    printf("Cons_%d: %d lines\n", (int)pthread_self(), i);
    *ret = i;
    pthread_exit(ret);
}


int main (int argc, char *argv[])
{
    pthread_t prod[100];
    pthread_t cons[100];
    int Nprod, Ncons;
    int rc;   long t;
    int *ret;
    int i;
    FILE *rfile;
    if (argc == 1) {
        printf("usage: ./prod_cons <readfile> #Producer #Consumer\n");
        exit (0);
    }
    so_t *share = malloc(sizeof(so_t));
    if (!share) {
        perror("malloc");
        exit(1);
    }
    memset(share, 0, sizeof(so_t));
    rfile = fopen((char *) argv[1], "r");
    if (rfile == NULL) {
        perror("rfile");
        free(share);
        exit(0);
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

    share->rfile = rfile;
    share->line = NULL;
    share->full = 0;
    pthread_mutex_init(&share->lock, NULL);
    pthread_cond_init(&share->cond, NULL);
    pthread_cond_init(&share->cond_empty, NULL);

    for (i = 0 ; i < Nprod ; i++)
        pthread_create(&prod[i], NULL, producer, share);
    for (i = 0 ; i < Ncons ; i++)
        pthread_create(&cons[i], NULL, consumer, share);
    printf("main continuing\n");

    for (i = 0 ; i < Ncons ; i++) {
        rc = pthread_join(cons[i], (void **) &ret);
        if (rc == 0 && ret) {
            printf("main: consumer_%d joined with %d\n", i, *ret);
            free(ret);
        } else {
            printf("main: consumer_%d join error %d\n", i, rc);
        }
    }
    for (i = 0 ; i < Nprod ; i++) {
        rc = pthread_join(prod[i], (void **) &ret);
        if (rc == 0 && ret) {
            printf("main: producer_%d joined with %d\n", i, *ret);
            free(ret);
        } else {
            printf("main: producer_%d join error %d\n", i, rc);
        }
    }

    pthread_mutex_destroy(&share->lock);
    pthread_cond_destroy(&share->cond);
    pthread_cond_destroy(&share->cond_empty);
    fclose(rfile);
    free(share);

    pthread_exit(NULL);
    exit(0);
}
