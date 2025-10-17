// prod_cons.c
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "prod_cons.h"

int main(int argc, char *argv[]){
	// Set time
	struct timespec start, end;
	clock_gettime(CLOCK_MONOTONIC, &start);

	// Default value of Nprod, Ncons, buf_size
	int Nprod = 1, Ncons = 1, buf_size = 8;

	// Get value of Nprod, Ncons, buf_size
	if(argc == 1){
		printf("Usage: ./prod_cons [readfile] [producer] [consumer] [buf_size]\n");
		exit(0);
	}
	if(argc >= 3){
		Nprod = atoi(argv[2]);
		if(Nprod <= 0){
			Nprod = 1;
		}else if(Nprod > 100){
			Nprod = 100;
		}
	}
	if(argc >= 4){
		Ncons = atoi(argv[3]);
		if(Ncons <= 0){
			Ncons = 1;
		}else if(Ncons > 100){
			Ncons = 100;
		}
	}
	if(argc >= 5){
		buf_size = atoi(argv[4]);
		if(buf_size <= 0){
			buf_size = 1;
		}else if(buf_size > 100){
			buf_size = 100;
		}
	}

	// Get file
	FILE *rfile = fopen(argv[1], "r");
	if(rfile == NULL){
		perror("fopen");
		exit(1);
	}

	// Initialize so
	so_t *so = calloc(1, sizeof(so_t));
	if(!so){
		perror("calloc");
		fclose(rfile);
		exit(1);
	}
	so->producers_alive = Nprod;
	so->buf_size = buf_size;
	so->buffer = calloc(buf_size, sizeof(char *));
	if(!so->buffer){
		perror("calloc");
		fclose(rfile);
		free(so);
		exit(1);
	}
	so->rfile = rfile;
	so->in = so->out = so->count = 0;
	for(int i = 0; i < 26; i++){
		so->letter_count[i] = 0L;
	}

	// Initialize cond & mutex
	pthread_cond_init(&so->not_full, NULL);
	pthread_cond_init(&so->not_empty, NULL);
	pthread_mutex_init(&so->lock, NULL);
	pthread_mutex_init(&so->file_lock, NULL);
	pthread_mutex_init(&so->stat_lock, NULL);

	// Create producer & consumer array
	pthread_t *prods = calloc(Nprod, sizeof(pthread_t));
	pthread_t *cons = calloc(Ncons, sizeof(pthread_t));
	if(!prods || !cons){
		perror("calloc");
		fclose(rfile);
		free(so->buffer);
		free(so);
		exit(1);
	}

	// Create producers
	for(int i = 0; i < Nprod; i++){
		int rc = pthread_create(&prods[i], NULL, producer, so);
		if(rc != 0){
			printf("Failed to create producer");
			exit(1);
		}
	}

	// Create consumers
	for(int i = 0; i < Ncons; i++){
		int rc = pthread_create(&cons[i], NULL, consumer, so);
		if(rc != 0){
			printf("Failed to create consumers");
			exit(1);
		}
	}

	// Wait for consumer thread to end
	for(int i = 0; i < Ncons; i++){
		void *res;
		int rc = pthread_join(cons[i], &res);
		if(rc != 0){
			printf("Failed to wait consumer");
			exit(1);
		}
	}

	// Wait for producer thread to end
	for(int i = 0; i < Nprod; i++){
		void *res;
		int rc = pthread_join(prods[i], &res);
		if(rc != 0){
			printf("Failed to wait producer");
			exit(1);
		}
	}

	// Get result
	print_char_stat(so->letter_count);

	// Get time
	clock_gettime(CLOCK_MONOTONIC, &end);
	double elapse = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
	printf("\n―――――――――― Running time ――――――――――\nㆍ%.6f seconds\n", elapse);

	// Clear & destroy
	fclose(so->rfile);
	free(so->buffer);
	free(so);
	pthread_cond_destroy(&so->not_full);
	pthread_cond_destroy(&so->not_empty);
	pthread_mutex_destroy(&so->lock);
	pthread_mutex_destroy(&so->file_lock);
	pthread_mutex_destroy(&so->stat_lock);
	
	return 0;
}