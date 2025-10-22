#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <ctype.h>

#define BUFFER_SIZE 10
#define MAX_LINE 1024

char* buffer[BUFFER_SIZE];
int in = 0;
int out = 0;  
int count = 0; 
int done = 0; 


pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t not_empty = PTHREAD_COND_INITIALIZER;
pthread_cond_t not_full = PTHREAD_COND_INITIALIZER;

pthread_mutex_t stat_mutex = PTHREAD_MUTEX_INITIALIZER;
int char_count[26] = {0}; 

char* filename; // input filename

void* producer(void* arg) {
    FILE* fp = fopen(filename, "r");
    if (!fp) {
        perror("fopen");
        exit(1);
    }

    char line[MAX_LINE];
    while (fgets(line, MAX_LINE, fp)) {
        char* copy = strdup(line); 
        pthread_mutex_lock(&mutex);
        while (count == BUFFER_SIZE)
            pthread_cond_wait(&not_full, &mutex);

        buffer[in] = copy;
        in = (in + 1) % BUFFER_SIZE;
        count++;

        pthread_cond_signal(&not_empty);
        pthread_mutex_unlock(&mutex);
    }

    fclose(fp);

    // Signal consumers to finish
    pthread_mutex_lock(&mutex);
    done = 1;
    pthread_cond_broadcast(&not_empty);
    pthread_mutex_unlock(&mutex);

    return NULL;
}

void* consumer(void* arg) {
    while (1) {
        pthread_mutex_lock(&mutex);
        while (count == 0 && !done)
            pthread_cond_wait(&not_empty, &mutex);

        if (count == 0 && done) {
            pthread_mutex_unlock(&mutex);
            break; // no more lines
        }

        char* line = buffer[out];
        out = (out + 1) % BUFFER_SIZE;
        count--;

        pthread_cond_signal(&not_full);
        pthread_mutex_unlock(&mutex);

               for (int i = 0; line[i]; i++) {
            char c = line[i];
            if (isalpha(c)) {
                c = tolower(c);
                pthread_mutex_lock(&stat_mutex);
                char_count[c - 'a']++;
                pthread_mutex_unlock(&stat_mutex);
            }
        }

        free(line);
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        printf("Usage: %s <filename> <num_consumers>\n", argv[0]);
        return 1;
    }

    filename = argv[1];
    int num_consumers = atoi(argv[2]);
    if (num_consumers <= 0) num_consumers = 1;

    pthread_t prod;
    pthread_t cons[num_consumers];

        pthread_create(&prod, NULL, producer, NULL);

    
    for (int i = 0; i < num_consumers; i++)
        pthread_create(&cons[i], NULL, consumer, NULL);

        pthread_join(prod, NULL);
    for (int i = 0; i < num_consumers; i++)
        pthread_join(cons[i], NULL);

       printf("\nCharacter counts:\n");
    for (int i = 0; i < 26; i++)
        printf("%c: %d\n", 'a' + i, char_count[i]);

    return 0;
}
