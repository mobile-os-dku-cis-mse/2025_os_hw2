// prod_cons.h
#ifndef PROD_CONS_H
#define PROD_CONS_H
#include <pthread.h>
#include <stdio.h>

// Shared object structure
typedef struct sharedobject{
	// Shared buffer
	char **buffer;
	int buf_size; // Shared buffer size
	int in; // Location that producer will write next data
	int out; // Location that consumer will read next data
	int count; // Number of data in current buffer
	
	// Number of producers still working
	int producers_alive;

	// Shared file pointer
	FILE *rfile;

	// Number of each alphabet character in the file
	long letter_count[26];

	// Cond & mutex for Synchronization
	pthread_cond_t not_full;
	pthread_cond_t not_empty;
	pthread_mutex_t lock;
	pthread_mutex_t file_lock;
	pthread_mutex_t stat_lock;
}so_t;

// Functions
void *producer(void *arg);
void *consumer(void *arg);
void print_char_stat(long count[26]);

#endif