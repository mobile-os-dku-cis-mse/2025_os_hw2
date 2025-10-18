#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <semaphore.h>

#define BUFFER_SIZE 100
//max amount of prods cant be more than BUFFER_SIZE

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
        so_t *so = arg;
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

                if (line == NULL) {
                        break;
                }
                //printf("Consumer_%x: [%03d] %s", (unsigned int)pthread_self(), i, line);
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

       // cons and prod thread creation
        for (i = 0 ; i < Nprod ; i++)
                pthread_create(&prod[i], NULL, producer, share);

        for (i = 0 ; i < Ncons ; i++)
                pthread_create(&cons[i], NULL, consumer, share);
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
        }   //without this, consumers may hang indefinitely

      // cons thread joining
        for (i = 0 ; i < Ncons ; i++) {
                rc = pthread_join(cons[i], (void **) &ret);
                printf("main: consumer_%d joined with %d\n", i, *ret);
                free(ret);
        }

 // Очистка ресурсов
        pthread_mutex_destroy(&share->lock);
    	sem_destroy(&share->empty);
    	sem_destroy(&share->full);
	fclose(rfile);
	free(share);
    	printf("All threads finished.\n");
        exit(0);
}
