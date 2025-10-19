#define _GNU_SOURCE
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>
#include <time.h>   

#define BUFCAP 64

typedef struct item {
    char *line;
    int lineno;
} item_t;


typedef struct sharedobject {
    FILE *rfile;
    item_t *buf;
    int bufcap;
    int head;
    int tail;
    int count; 
    int active_producers;

    // file and buffer synchronization 
    pthread_mutex_t file_lock;   
    pthread_mutex_t lock;        
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    long stats[26];   //A to Z           
} so_t;


static void buffer_push(so_t *so, char *line, int lineno) {
    so->buf[so->tail].line = line;
    so->buf[so->tail].lineno = lineno;
    so->tail = (so->tail + 1) % so->bufcap;
    so->count++;
}

static item_t buffer_pop(so_t *so) {
    item_t it = so->buf[so->head];
    so->head = (so->head + 1) % so->bufcap;
    so->count--;
    return it;
}

void *producer(void *arg) {
    so_t *so = arg;
    int *ret = malloc(sizeof(int));
    int lines_read = 0;

    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    int lineno = 0;

    while (1) {
        pthread_mutex_lock(&so->file_lock);
        read = getdelim(&line, &len, '\n', so->rfile);
        pthread_mutex_unlock(&so->file_lock);

        if (read == -1) { //If the  find ends or mistake
            free(line);
            line = NULL;
            break;
        }

        char *entry = strdup(line);
        if (!entry) {
            perror("strdup");
            free(line);
            break;
        }

        pthread_mutex_lock(&so->lock);
        while (so->count == so->bufcap) {
            // If buffer full, we wait 
            pthread_cond_wait(&so->not_full, &so->lock);
        }
        buffer_push(so, entry, lineno);
        // signal that there's data
        pthread_cond_signal(&so->not_empty);
        pthread_mutex_unlock(&so->lock);

        lineno++;
        lines_read++;
    }

    pthread_mutex_lock(&so->lock);
    so->active_producers--;
    pthread_cond_broadcast(&so->not_empty);
    pthread_mutex_unlock(&so->lock);

    printf("Prod_%x: %d lines\n", (unsigned int)pthread_self(), lines_read);
    *ret = lines_read;
    pthread_exit(ret);
}

void *consumer(void *arg) {
    so_t *so = arg;
    int *ret = malloc(sizeof(int));
    int lines_consumed = 0;

    while (1) {
        pthread_mutex_lock(&so->lock);
        // wait while buffer empty and producers active 
        while (so->count == 0 && so->active_producers > 0) {
            pthread_cond_wait(&so->not_empty, &so->lock);
        }

        //If doesn't happen, we unlock
        if (so->count == 0 && so->active_producers == 0) {
            pthread_mutex_unlock(&so->lock);
            break;
        }

        item_t it = buffer_pop(so);
        pthread_cond_signal(&so->not_full); //Signal to say not full
        pthread_mutex_unlock(&so->lock);

        char *line = it.line;
        int lineno = it.lineno;

        printf("Cons_%x: [%02d:%02d] %s",
               (unsigned int)pthread_self(), lines_consumed, lineno, line);

        // update stats 
        for (char *p = line; *p; ++p) {
            unsigned char c = (unsigned char)*p;
            if (isalpha(c)) {
                int idx = toupper(c) - 'A';
                if (0 <= idx && idx < 26) {
                    pthread_mutex_lock(&so->file_lock);
                    so->stats[idx]++;
                    pthread_mutex_unlock(&so->file_lock);
                }
            }
        }

        free(line);
        lines_consumed++;
    }

    printf("Cons_%x: %d lines\n", (unsigned int)pthread_self(), lines_consumed);
    *ret = lines_consumed;
    pthread_exit(ret);
}

int main (int argc, char *argv[])
{
    pthread_t prod[100];
    pthread_t cons[100];
    int Nprod = 1, Ncons = 1;
    int rc;
    int *ret;
    int i;
    FILE *rfile;
    struct timespec start, end;
    double elapsed;

    clock_gettime(CLOCK_MONOTONIC, &start);

    if (argc == 1) {
        printf("usage: ./prod_cons <readfile> #Producer #Consumer\n");
        exit (0);
    }

    if (argv[2] != NULL) {
        Nprod = atoi(argv[2]);
        if (Nprod > 100) Nprod = 100;
        if (Nprod <= 0) Nprod = 1;
    }
    if (argv[3] != NULL) {
        Ncons = atoi(argv[3]);
        if (Ncons > 100) Ncons = 100;
        if (Ncons <= 0) Ncons = 1;
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

    share->rfile = rfile;
    share->bufcap = BUFCAP;
    share->buf = calloc(share->bufcap, sizeof(item_t));
    if (!share->buf) {
        perror("calloc");
        fclose(rfile);
        free(share);
        exit(1);
    }
    share->head = share->tail = share->count = 0;
    share->active_producers = Nprod;
    for (i = 0; i < 26; ++i) share->stats[i] = 0;

    pthread_mutex_init(&share->file_lock, NULL);
    pthread_mutex_init(&share->lock, NULL);
    pthread_cond_init(&share->not_empty, NULL);
    pthread_cond_init(&share->not_full, NULL);

    // Producers creation
    for (i = 0 ; i < Nprod ; i++) {
        rc = pthread_create(&prod[i], NULL, producer, share);
        if (rc) {
            fprintf(stderr, "error: pthread_create producer %d -> %d\n", i, rc);
            exit(1);
        }
    }
    // Consumers creation
    for (i = 0 ; i < Ncons ; i++) {
        rc = pthread_create(&cons[i], NULL, consumer, share);
        if (rc) {
            fprintf(stderr, "error: pthread_create consumer %d -> %d\n", i, rc);
            exit(1);
        }
    }

    printf("main continuing\n");

    // join consumers
    int total_consumed = 0;
    for (i = 0 ; i < Ncons ; i++) {
        rc = pthread_join(cons[i], (void **) &ret);
        if (rc == 0) {
            printf("main: consumer_%d joined with %d\n", i, *ret);
            total_consumed += *ret;
            free(ret);
        }
    }

    // join producers 
    int total_produced = 0;
    for (i = 0 ; i < Nprod ; i++) {
        rc = pthread_join(prod[i], (void **) &ret);
        if (rc == 0) {
            printf("main: producer_%d joined with %d\n", i, *ret);
            total_produced += *ret;
            free(ret);
        }
    }
    

    // print  stats
    printf("\n==== Character statistics (A-Z) ====\n");
    long sum = 0;
    for (i = 0; i < 26; ++i) {
        printf("%c: %ld\n", 'A' + i, share->stats[i]);
        sum += share->stats[i];
    }
    clock_gettime(CLOCK_MONOTONIC, &end);     
    elapsed = (end.tv_sec - start.tv_sec);
    elapsed += (end.tv_nsec - start.tv_nsec) / 1e9;

    printf("Total letters counted: %ld\n", sum);
    printf("Total lines produced: %d, consumed: %d\n", total_produced, total_consumed);
    printf("Elapsed time: %.6f seconds\n", elapsed);
    //Mantainance
    fclose(share->rfile);
    free(share->buf);
    pthread_mutex_destroy(&share->file_lock);
    pthread_mutex_destroy(&share->lock);
    pthread_cond_destroy(&share->not_empty);
    pthread_cond_destroy(&share->not_full);
    free(share);

    pthread_exit(NULL);
    return 0;
}
