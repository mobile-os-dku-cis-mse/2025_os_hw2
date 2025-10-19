#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <ctype.h>

#define BUFSIZE 10
#define MAX_STRING_LENGTH 30
#define ASCII_SIZE 256

// Global stats
int stat_len[MAX_STRING_LENGTH];
int stat_ascii[ASCII_SIZE];

typedef struct sharedobject {
    FILE *rfile;
    char *lines[BUFSIZE];
    int linenum[BUFSIZE];
    int in, out, count;
    pthread_mutex_t lock;
    pthread_cond_t cond_full;
    pthread_cond_t cond_empty;
    pthread_mutex_t file_lock;
    int done;
    int active_producers;
} so_t;

void update_stats(const char *line) {
    char *copy = strdup(line);
    if (!copy) return;

    char *brka = NULL;
    char *sep = "{}()[],;\" \n\t^";
    char *substr = strtok_r(copy, sep, &brka);

    while (substr) {
        size_t len = strlen(substr);
        if (len > 0) {
            if (len > MAX_STRING_LENGTH) len = MAX_STRING_LENGTH;
            stat_len[len - 1]++;

            for (size_t i = 0; i < strlen(substr); i++) {
                unsigned char c = substr[i];
                if (c > 1 && c < 256)
                    stat_ascii[c]++;
            }
        }
        substr = strtok_r(NULL, sep, &brka);
    }

    free(copy);
}

void *producer(void *arg) {
    so_t *so = arg;
    int *ret = malloc(sizeof(int));
    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    int i = 0;

    while (1) {
        pthread_mutex_lock(&so->file_lock);
        read = getdelim(&line, &len, '\n', so->rfile);
        pthread_mutex_unlock(&so->file_lock);

        if (read == -1)
            break;

        pthread_mutex_lock(&so->lock);
        while (so->count == BUFSIZE) {
            pthread_cond_wait(&so->cond_full, &so->lock);
        }

        so->lines[so->in] = strdup(line);
        so->linenum[so->in] = i;
        so->in = (so->in + 1) % BUFSIZE;
        so->count++;

        pthread_cond_signal(&so->cond_empty);
        pthread_mutex_unlock(&so->lock);
        i++;
    }

    pthread_mutex_lock(&so->lock);
    so->active_producers--;
    if (so->active_producers == 0)
        so->done = 1;
    pthread_cond_broadcast(&so->cond_empty);
    pthread_mutex_unlock(&so->lock);

    free(line);
    printf("Prod_%x: %d lines\n", (unsigned int)pthread_self(), i);
    *ret = i;
    pthread_exit(ret);
}

void *consumer(void *arg) {
    so_t *so = arg;
    int *ret = malloc(sizeof(int));
    int i = 0;

    while (1) {
        pthread_mutex_lock(&so->lock);
        while (so->count == 0 && !so->done)
            pthread_cond_wait(&so->cond_empty, &so->lock);

        if (so->count == 0 && so->done) {
            pthread_mutex_unlock(&so->lock);
            break;
        }

        char *line = so->lines[so->out];
        int lnum = so->linenum[so->out];
        so->out = (so->out + 1) % BUFSIZE;
        so->count--;

        pthread_cond_signal(&so->cond_full);
        pthread_mutex_unlock(&so->lock);

        printf("Cons_%x: [%02d:%02d] %s", (unsigned int)pthread_self(), i, lnum, line);

        pthread_mutex_lock(&so->lock);
        update_stats(line);
        pthread_mutex_unlock(&so->lock);

        free(line);
        i++;
        usleep(20000);
    }

    printf("Cons_%x: %d lines\n", (unsigned int)pthread_self(), i);
    *ret = i;
    pthread_exit(ret);
}

int main(int argc, char *argv[]) {
    pthread_t prod[100];
    pthread_t cons[100];
    int Nprod, Ncons;
    int *ret;
    int i, j;
    FILE *rfile;

    if (argc == 1) {
        printf("usage: ./word_count <readfile> #Producer #Consumer\n");
        exit(0);
    }

    so_t *share = malloc(sizeof(so_t));
    memset(share, 0, sizeof(so_t));

    rfile = fopen(argv[1], "r");
    if (rfile == NULL) {
        perror("rfile");
        exit(0);
    }

    Nprod = (argv[2] ? atoi(argv[2]) : 1);
    if (Nprod <= 0) Nprod = 1;
    if (Nprod > 100) Nprod = 100;

    Ncons = (argv[3] ? atoi(argv[3]) : 1);
    if (Ncons <= 0) Ncons = 1;
    if (Ncons > 100) Ncons = 100;

    share->rfile = rfile;
    pthread_mutex_init(&share->lock, NULL);
    pthread_mutex_init(&share->file_lock, NULL);
    pthread_cond_init(&share->cond_full, NULL);
    pthread_cond_init(&share->cond_empty, NULL);
    share->active_producers = Nprod;

    memset(stat_len, 0, sizeof(stat_len));
    memset(stat_ascii, 0, sizeof(stat_ascii));

    for (i = 0; i < Nprod; i++)
        pthread_create(&prod[i], NULL, producer, share);
    for (i = 0; i < Ncons; i++)
        pthread_create(&cons[i], NULL, consumer, share);

    printf("main continuing\n");

    for (i = 0; i < Ncons; i++) {
        pthread_join(cons[i], (void **)&ret);
        printf("main: consumer_%d joined with %d\n", i, *ret);
        free(ret);
    }
    for (i = 0; i < Nprod; i++) {
        pthread_join(prod[i], (void **)&ret);
        printf("main: producer_%d joined with %d\n", i, *ret);
        free(ret);
    }

    // Char Statistics
    int sum = 0;
    for (i = 0; i < MAX_STRING_LENGTH; i++)
        sum += stat_len[i];

    printf("\n*** print out distributions *** \n");
    printf("  #ch  freq\n");
    for (i = 0; i < MAX_STRING_LENGTH; i++) {
        int num_star = (sum ? stat_len[i] * 80 / sum : 0);
        printf("[%3d]: %4d \t", i + 1, stat_len[i]);
        for (int j = 0; j < num_star; j++) printf("*");
        printf("\n");
    }

    printf("\n");
    printf("       A        B        C        D        E        F        G        H        I        J        K        L        M        N        O        P        Q        R        S        T        U        V        W        X        Y        Z\n");
    printf("%8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d\n",
           stat_ascii['A'] + stat_ascii['a'], stat_ascii['B'] + stat_ascii['b'],
           stat_ascii['C'] + stat_ascii['c'], stat_ascii['D'] + stat_ascii['d'],
           stat_ascii['E'] + stat_ascii['e'], stat_ascii['F'] + stat_ascii['f'],
           stat_ascii['G'] + stat_ascii['g'], stat_ascii['H'] + stat_ascii['h'],
           stat_ascii['I'] + stat_ascii['i'], stat_ascii['J'] + stat_ascii['j'],
           stat_ascii['K'] + stat_ascii['k'], stat_ascii['L'] + stat_ascii['l'],
           stat_ascii['M'] + stat_ascii['m'], stat_ascii['N'] + stat_ascii['n'],
           stat_ascii['O'] + stat_ascii['o'], stat_ascii['P'] + stat_ascii['p'],
           stat_ascii['Q'] + stat_ascii['q'], stat_ascii['R'] + stat_ascii['r'],
           stat_ascii['S'] + stat_ascii['s'], stat_ascii['T'] + stat_ascii['t'],
           stat_ascii['U'] + stat_ascii['u'], stat_ascii['V'] + stat_ascii['v'],
           stat_ascii['W'] + stat_ascii['w'], stat_ascii['X'] + stat_ascii['x'],
           stat_ascii['Y'] + stat_ascii['y'], stat_ascii['Z'] + stat_ascii['z']);

    fclose(rfile);
    pthread_exit(NULL);
}
