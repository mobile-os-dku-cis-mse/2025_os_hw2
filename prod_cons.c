#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>


#define ALPHA_SIZE 26
#define BUFFER_SIZE 1500000

typedef struct consumer_result{
	int line_processed;
	int alpha_count[ALPHA_SIZE];
} cons_res_t;

typedef struct buffer_item{
	char *line;
	int linenum;
}buffer_item_t;

typedef struct sharedobject {
	FILE *rfile;
	buffer_item_t buffer[BUFFER_SIZE];
	int read_pos;
	int write_pos;
	int count;
	pthread_mutex_t lock;
	int eof_reached;
	pthread_cond_t not_full;
	pthread_cond_t not_empty;
} so_t;

int char_to_index(char c){
	if(c >= 'a' && c<= 'z'){
		return c - 'a';
	}else if(c >= 'A' && c <='Z'){
		return c - 'A';
	}
	return -1;
}

void *producer(void *arg) {
	so_t *so = arg;
	int *ret = malloc(sizeof(int));
	FILE *rfile = so->rfile;
	int lines_read = 0;
	char *line = NULL;
	size_t len = 0;
	ssize_t read = 0;
	static int wait_count = 0;

	while (1) {
		read = getdelim(&line, &len, '\n', rfile);
		if (read == -1){
			pthread_mutex_lock(&so->lock);
			so->eof_reached = 1;
			pthread_cond_broadcast(&so->not_empty);
			pthread_mutex_unlock(&so->lock);
			printf("Producer waited %d times\n", wait_count);
			break;
		}
		char *line_copy = strdup(line);
		if(line_copy == NULL){
			perror("strdup falied in producer");
			break;
		}
		pthread_mutex_lock(&so->lock);
		while (so->count == BUFFER_SIZE){
			wait_count++;
			pthread_cond_wait(&so->not_full, &so->lock);
		}
		so->buffer[so->write_pos].line = line_copy;
		so->buffer[so->write_pos].linenum = lines_read;
		so->write_pos = (so->write_pos + 1) % BUFFER_SIZE;
		so->count++;
		pthread_cond_signal(&so->not_empty);
		pthread_mutex_unlock(&so->lock);
		lines_read++;
	}
	if(line != NULL){
		free(line);
	}
	printf("Prod_%x: %d lines\n", (unsigned int)pthread_self(), lines_read);
	*ret = lines_read;
	pthread_exit(ret);
}

void *consumer(void *arg) {
	so_t *so = arg;
	cons_res_t *res = malloc(sizeof(cons_res_t));
	if(res == NULL){
		perror("malloc failed in consumer");
		pthread_exit(NULL);
	}
	memset(res, 0, sizeof(cons_res_t));
	int lines_processed;

	while (1) {
		pthread_mutex_lock(&so->lock);
		while(so->count == 0 && !so->eof_reached){
			pthread_cond_wait(&so->not_empty,&so->lock);
		}
		if (so->count == 0 && so->eof_reached) {
			pthread_cond_broadcast(&so->not_empty);
			pthread_mutex_unlock(&so->lock);
			break;
		}
		char *line = so->buffer[so->read_pos].line;
		int linenum = so->buffer[so->read_pos].linenum;
		so->buffer[so->read_pos].line = NULL;
		so->read_pos = (so->read_pos + 1) % BUFFER_SIZE;
		so->count--;
		pthread_cond_signal(&so->not_full);
		pthread_mutex_unlock(&so->lock);

		int len = strlen(line);
		for(int k = 0; k<len; k++){
			int index = char_to_index(line[k]);
			if(index != -1){
				res->alpha_count[index]++;
			}
		}
		printf("Cons_%x: [%02d:%02d] %s",
			(unsigned int)pthread_self(), lines_processed, linenum, line);
		free(line);
		lines_processed++;
	}
	printf("Cons_%x: %d lines\n", (unsigned int)pthread_self(), lines_processed);
	res->line_processed = lines_processed;
	pthread_exit(res);
}


int main (int argc, char *argv[]){
	int final_alpha_count[ALPHA_SIZE];
	memset(final_alpha_count, 0, sizeof(final_alpha_count));
	pthread_t prod[100];
	pthread_t cons[100];
	int Nprod, Ncons;
	int rc;   long t;
	int *ret;
	int i;
	FILE *rfile;
	struct timeval start_time, end_time;
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

	share->rfile = rfile;
	share->read_pos = 0;
	share->write_pos = 0;
	share->eof_reached = 0;
	share->count = 0;
	pthread_mutex_init(&share->lock, NULL);
	pthread_cond_init(&share->not_full, NULL);
	pthread_cond_init(&share->not_empty, NULL);
	printf("Starting with %d producer(s) and %d consumer(s) (buffer size: %d)\n",
		   Nprod, Ncons, BUFFER_SIZE);
	gettimeofday(&start_time, NULL);
	for (i = 0 ; i < Nprod ; i++)
		pthread_create(&prod[i], NULL, producer, share);
	for (i = 0 ; i < Ncons ; i++)
		pthread_create(&cons[i], NULL, consumer, share);
	printf("main continuing\n");

	for (i = 0 ; i < Ncons ; i++) {
		cons_res_t *res;
		rc = pthread_join(cons[i], (void **) &res);
		for(int j=0; j< ALPHA_SIZE; j++){
			final_alpha_count[j] += res->alpha_count[j];
		}
		printf("main: consumer_%d joined with %d\n", i, res->line_processed);
		free(res);
	}
	for (i = 0 ; i < Nprod ; i++) {
		rc = pthread_join(prod[i], (void **) &ret);
		printf("main: producer_%d joined with %d\n", i, *ret);
		free(ret);
	}

	gettimeofday(&end_time, NULL);
	double elapsed = (end_time.tv_sec - start_time.tv_sec) +
					 (end_time.tv_usec - start_time.tv_usec) / 1e6;
	printf("\n*** Final Alphabet Character Count ***\n");
	for (i = 0; i < ALPHA_SIZE; i++){
		printf("%c = %d개", 'A' + i, final_alpha_count[i]);
		if ((i + 1) % 5 == 0 || i == ALPHA_SIZE - 1){
			printf("\n");
		}else{
			printf(" | ");
		}
	}
	printf("\n");
	printf("Execution time: %.3f seconds\n", elapsed);
	fclose(rfile);
	pthread_mutex_destroy(&share->lock);
	pthread_cond_destroy(&share->not_full);
	pthread_cond_destroy(&share->not_empty);
	for (i = 0; i < BUFFER_SIZE; i++)
	{
		if (share->buffer[i].line != NULL)
		{
			free(share->buffer[i].line);
		}
	}
	free(share);
	pthread_exit(NULL);
	exit(0);
}