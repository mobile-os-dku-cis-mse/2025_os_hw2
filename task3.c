#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <semaphore.h>

#define BUFFER_SIZE 10
//max amount of prods cant be more than BUFFER_SIZE
#define MAX_STRING_LENGTH 30
#define ASCII_SIZE	256



typedef struct sharedobject {
        FILE *rfile;
        char *lines[BUFFER_SIZE];   //round buffer for lines
    	int head;                   // read index
    	int tail;                   // write index
    	int count; 
	sem_t empty;                
    	sem_t full;  
        pthread_mutex_t lock;
} so_t;

typedef struct stats {
    int stat[MAX_STRING_LENGTH];
    int stat2[ASCII_SIZE];
    pthread_mutex_t stat_lock;
} stats_t;

typedef struct consumer_args {
    so_t *so;
    stats_t *stats;
} cons_arg_t;

void stat_line(char *line, stats_t *stats) {
    if (!line || !stats) return; // input validation

    char *copy = strdup(line); // duplicate line to avoid modifying the original
    if (!copy) return;

    char *brka = NULL;
    char *sep = "{}()[],;\" \n\t^"; 
    char *substr = strtok_r(copy, sep, &brka);
    
        // process each substring (word in the line)
    while (substr != NULL) { 
    size_t length = strlen(substr);
    if (length > 0) {
        if (length > MAX_STRING_LENGTH) length = MAX_STRING_LENGTH;        
        // update stats with mutex protection
        pthread_mutex_lock(&stats->stat_lock);
        stats->stat[length - 1]++;
        for (size_t i = 0; i < length; i++) {
                unsigned char c = (unsigned char)substr[i];
                if (c < ASCII_SIZE)
                    stats->stat2[c]++;
        }
        pthread_mutex_unlock(&stats->stat_lock);
        }
        substr = strtok_r(NULL, sep, &brka);
    }
    free(copy);

}

void *producer(void *arg) {
        so_t *so = arg;
        int *ret = malloc(sizeof(int));
        FILE *rfile = so->rfile;
        int i = 0;
        char *line = NULL;
        size_t len = 0;
        ssize_t read = 0;

        while (1) {
                //pthread_mutex_lock(&so->file_lock);
                read = getdelim(&line, &len, '\n', rfile);
                //pthread_mutex_unlock(&so->file_lock);
                //experience shows that locking file access is not necessary here
                if (read == -1) {
			sem_wait(&so->empty);
    			pthread_mutex_lock(&so->lock);
    			so->lines[so->tail] = NULL;
    			so->tail = (so->tail + 1) % BUFFER_SIZE;
    			so->count++;
    			pthread_mutex_unlock(&so->lock);
    			sem_post(&so->full);
                	break;
                } // needeed to signal consumers to stop when EOF is reached

		sem_wait(&so->empty);              
        	pthread_mutex_lock(&so->lock);

                so->lines[so->tail] = strdup(line);
        	so->tail = (so->tail + 1) % BUFFER_SIZE;
        	so->count++;

		pthread_mutex_unlock(&so->lock);
        	sem_post(&so->full); 
		i++;

        }
        free(line);
    	//printf("Producer_%x: produced %d lines\n", (unsigned int)pthread_self(), i);
	*ret = i;
	pthread_exit(ret);
}

void *consumer(void *arg) {
        cons_arg_t *cons_arg = arg;
        so_t *so = cons_arg->so;
        stats_t *stats = cons_arg->stats;
        int *ret = malloc(sizeof(int));
        int i = 0;
        char *line;

        while (1) {
                //printf("Cons_%x: waiting\n", (unsigned int)pthread_self());
            	sem_wait(&so->full);
        	pthread_mutex_lock(&so->lock);
                //printf("Cons_%x: consuming\n", (unsigned int)pthread_self());

		line = so->lines[so->head];
        	so->head = (so->head + 1) % BUFFER_SIZE;
        	so->count--;

		pthread_mutex_unlock(&so->lock);
		sem_post(&so->empty);

                if (line == NULL) break;
                        
                
               // printf("Consumer_%x: [%03d] %s", (unsigned int)pthread_self(), i, line);
                stat_line(line, stats);
        	free(line);
        	i++;
        }
        //printf("Cons: %d lines\n", i);
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
        memset(share, 0, sizeof(so_t));
        rfile = fopen((char *) argv[1], "r");

        stats_t *stats = malloc(sizeof(stats_t));
        memset(stats, 0, sizeof(stats_t));
        

        if (rfile == NULL) {
                perror("rfile");
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

        // Shared object initialization
        share->rfile = rfile;
        share->head = 0;
	share->tail = 0;	
	share->count = 0;

        //semaphore and mutex initialization
	sem_init(&share->empty, 0, BUFFER_SIZE); 
	sem_init(&share->full, 0, 0);            
        pthread_mutex_init(&share->lock, NULL);
        pthread_mutex_init(&stats->stat_lock, NULL);

       // cons and prod thread creation
        for (i = 0 ; i < Nprod ; i++)
                pthread_create(&prod[i], NULL, producer, share);
        
        //consumer args (statistics and shared object) preparation and thread creation
        cons_arg_t *cons_args = malloc(sizeof(cons_arg_t) * Ncons);
        for (i = 0 ; i < Ncons ; i++){
                cons_args[i].so = share;
                cons_args[i].stats = stats;
                pthread_create(&cons[i], NULL, consumer, &cons_args[i]);
        }
        printf("main continuing\n");

        // prods thread joining
        for (i = 0 ; i < Nprod ; i++) {
                rc = pthread_join(prod[i], (void **) &ret);
                printf("main: producer_%d joined with %d\n", i, *ret);
                free(ret);
        }

        // cons thread termination signaling
        for (int j = 0; j < Ncons; ++j) {
                sem_wait(&share->empty);
                pthread_mutex_lock(&share->lock);

                share->lines[share->tail] = NULL;
                share->tail = (share->tail + 1) % BUFFER_SIZE;
                share->count++;

                pthread_mutex_unlock(&share->lock);
                sem_post(&share->full);
        }   //without this, consumers may hang indefinitely (if they are waiting on full semaphore)

        // cons thread joining
        for (i = 0 ; i < Ncons ; i++) {
                rc = pthread_join(cons[i], (void **) &ret);
                printf("main: consumer_%d joined with %d\n", i, *ret);
                free(ret);
        }

        // print stats
        printf("*** Character statistics ***\n");
        for (int i = 0; i < 26; i++) {
        char letter = 'A' + i;
        printf("%c: %d\n", letter, stats->stat2[letter] + stats->stat2[letter + 32]);
        }       

        printf("*** Word length distribution ***\n");
        for (int i = 0; i < MAX_STRING_LENGTH; i++) {
        printf("Length %2d: %d\n", i + 1, stats->stat[i]);
        }

        //cleanup
        pthread_mutex_destroy(&share->lock);
        pthread_mutex_destroy(&stats->stat_lock);
    	sem_destroy(&share->empty);
    	sem_destroy(&share->full);
        free(cons_args);
	fclose(rfile);
	free(share);
        free(stats);
    	printf("All threads finished.\n");
        exit(0);

        // all the outputs in consumer and producer threads are commented out to reduce completion time
}