// pthread.c
#define _GNU_SOURCE // Needed for getline()
#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "prod_cons.h"

void *producer(void *arg){
    // Shared object pointer & local variable
    so_t *so = (so_t *)arg;
    char *line = NULL;
    int produced = 0;
    size_t len = 0;
    ssize_t n;

    while(1){
        // Lock for multiple producer cannot read file simultaneously & Get line
        pthread_mutex_lock(&so->file_lock);
        n = getline(&line, &len, so->rfile);
        pthread_mutex_unlock(&so->file_lock);
        // EOF or error
        if(n == -1){
            break;
        }
        // Copy line to new memory for prevent getline() overwriting shared buffer
        char *entry = strdup(line);
        if(!entry){
            perror("strdup");
            break;
        }

        // Lock for producers and consumers cannot access shared buffer simultaneously
        pthread_mutex_lock(&so->lock);
        // Wait if shared buffer is full
        while(so->count == so->buf_size){
            pthread_cond_wait(&so->not_full, &so->lock);
        }
        // Save data into shared buffer
        so->buffer[so->in] = entry;
        so->in = (so->in + 1) % so->buf_size;
        so->count++;
        produced++;
        // Signal consumers & unlock
        pthread_cond_signal(&so->not_empty);
        pthread_mutex_unlock(&so->lock);
    }

    // Free temporary buffer 'line' used by getline()
    if(line){
        free(line);
    }

    // Lock for producers and consumers cannot access shared variables simultaneously
    pthread_mutex_lock(&so->lock);
    so->producers_alive--;
    // Wake up all consumers so they can re-check buffer state or detect that producers have finished
    pthread_cond_broadcast(&so->not_empty);
    pthread_mutex_unlock(&so->lock);

    return NULL;
}

void *consumer(void *arg){
    // Shared object pointer & local variable
    so_t *so = (so_t *)arg;
    int processed = 0;
    int local_counts[26];
    memset(local_counts, 0, sizeof(local_counts));

    while(1){
        // Lock for producers and consumers cannot access shared buffer simultaneously
        pthread_mutex_lock(&so->lock);
        while(so->count == 0 && so->producers_alive > 0){
            // Wait if shared buffer is empty, but producer is running
            pthread_cond_wait(&so->not_empty, &so->lock);
        }
        // Exit if shared buffer is empty and producers are not alive
        if(so->count == 0 && so->producers_alive == 0){
            pthread_mutex_unlock(&so->lock);
            break;
        }

        // Remove data from shared buffer
        char *entry = so->buffer[so->out];
        so->buffer[so->out] = NULL;
        so->out = (so->out + 1) % so->buf_size;
        so->count--;
        // Signal producer & unlock
        pthread_cond_signal(&so->not_full);
        pthread_mutex_unlock(&so->lock);

        // Print line & count letter
        if(entry){
            printf("%x: %s", (unsigned int)pthread_self(), entry);
            for(char *p = entry; *p; ++p){
                if(isalpha((unsigned char)*p)){
                    char c = tolower((unsigned char)*p);
                    int idx = c - 'a';
                    if(idx >= 0 && idx < 26){
                        local_counts[idx]++;
                    }
                }
            }
            free(entry);
            processed++;
        }
    }

    // Lock for multiple consumer cannot add counted letters to global array simultaneously
    pthread_mutex_lock(&so->stat_lock);
    for(int i = 0; i < 26; ++i){
        so->letter_count[i] += local_counts[i];
    }
    pthread_mutex_unlock(&so->stat_lock);

    return NULL;
}