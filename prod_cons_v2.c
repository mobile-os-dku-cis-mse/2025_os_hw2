#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdatomic.h>

#define PROD_MULTIPLICATOR 2

typedef struct so_producer_s
{
	char *line;
	pthread_mutex_t lock;
	bool is_full;
	bool finished;
	int actual_linenum;
} so_producer_t;

typedef struct sharedobject
{
	FILE *rfile;
	atomic_int linenum;
	so_producer_t **prod;
	int Ndataprod;
	bool all_done;
} so_t;

void *producer(void *arg)
{
	so_t *so = arg;
	int *ret = malloc(sizeof(int));
	FILE *rfile = so->rfile;
	int i = 0;
	char *line = NULL;
	size_t len = 0;
	ssize_t read = 0;
	int id_dataprod = 0;
	bool done = false;
	int actual_linenum = 0;
	while (1)
	{
		id_dataprod = 0;
		done = false;
		read = getdelim(&line, &len, '\n', rfile);
		if (read == -1)
		{
			break;
		}
		atomic_fetch_add(&so->linenum, 1);
		actual_linenum = so->linenum;
		while (!done)
		{
			if (id_dataprod >= so->Ndataprod)
			{
				id_dataprod = 0;
			}
			if (so->prod[id_dataprod]->is_full == true)
			{
				id_dataprod++;
				continue;
			}
			if (pthread_mutex_trylock(&so->prod[id_dataprod]->lock) == 0)
			{
				so->prod[id_dataprod]->finished = false;
				so->prod[id_dataprod]->line = strdup(line);
				so->prod[id_dataprod]->is_full = true;
				so->prod[id_dataprod]->actual_linenum = actual_linenum;
				pthread_mutex_unlock(&so->prod[id_dataprod]->lock);
				i++;
				done = true;
				id_dataprod++;
			}
			else
			{
				id_dataprod++;
				continue;
			}
		}
	}
	free(line);
	printf("Prod_%x: %d lines\n", (unsigned int)pthread_self(), i);
	*ret = i;
	pthread_exit(ret);
}

void *consumer(void *arg)
{
	so_t *so = arg;
	int *ret = malloc(sizeof(int));
	int i = 0;
	char *line;
	int id_dataprod = 0;

	while (1)
	{
		if (id_dataprod >= so->Ndataprod)
		{
			if (so->all_done)
				break;
			id_dataprod = 0;
		}
		if (so->prod[id_dataprod]->is_full == false)
		{
			id_dataprod++;
			continue;
		}
		if (pthread_mutex_trylock(&so->prod[id_dataprod]->lock) == 0)
		{
			line = so->prod[id_dataprod]->line;
			printf("Cons_%x: [%02d:%02d] %s",
				   (unsigned int)pthread_self(), i, so->linenum, line);
			free(so->prod[id_dataprod]->line);
			so->prod[id_dataprod]->line = NULL;
			so->prod[id_dataprod]->is_full = false;
			so->prod[id_dataprod]->finished = true;
			pthread_mutex_unlock(&so->prod[id_dataprod]->lock);
			i++;
			id_dataprod++;
		}
		else
		{
			id_dataprod++;
		}
	}
	printf("Cons: %d lines\n", i);
	*ret = i;
	pthread_exit(ret);
}

int main(int argc, char *argv[])
{
	pthread_t prod[100];
	pthread_t cons[100];
	int Nprod, Ncons;
	int rc;
	long t;
	int *ret;
	int i;
	FILE *rfile;
	bool all_is_finished = false;

	if (argc == 1)
	{
		printf("usage: ./prod_cons <readfile> #Producer #Consumer\n");
		exit(0);
	}

	so_t *share = malloc(sizeof(so_t));
	memset(share, 0, sizeof(so_t));
	rfile = fopen((char *)argv[1], "r");
	if (rfile == NULL)
	{
		perror("rfile");
		exit(0);
	}

	if (argv[2] != NULL)
	{
		Nprod = atoi(argv[2]);
		if (Nprod > 100)
			Nprod = 100;
		if (Nprod == 0)
			Nprod = 1;
	}
	else
		Nprod = 1;

	if (argv[3] != NULL)
	{
		Ncons = atoi(argv[3]);
		if (Ncons > 100)
			Ncons = 100;
		if (Ncons == 0)
			Ncons = 1;
	}
	else
		Ncons = 1;

	share->rfile = rfile;
	share->linenum = 0;
	share->Ndataprod = Nprod * PROD_MULTIPLICATOR;
	share->prod = malloc(Nprod * PROD_MULTIPLICATOR * sizeof(so_producer_t *));
	for (i = 0; i < Nprod * PROD_MULTIPLICATOR; i++)
	{
		share->prod[i] = malloc(sizeof(so_producer_t));
		memset(share->prod[i], 0, sizeof(so_producer_t));
		share->prod[i]->line = NULL;
		share->prod[i]->is_full = false;
		share->prod[i]->finished = true;
		share->prod[i]->actual_linenum = 0;
		pthread_mutex_init(&share->prod[i]->lock, NULL);
	}

	for (i = 0; i < Nprod; i++)
	{
		pthread_create(&prod[i], NULL, producer, share);
	}
	for (i = 0; i < Ncons; i++)
		pthread_create(&cons[i], NULL, consumer, share);
	printf("main continuing\n");

	for (i = 0; i < Nprod; i++)
	{
		rc = pthread_join(prod[i], (void **)&ret);
		printf("main: producer_%d joined with %d\n", i, *ret);
		free(ret);
	}
	while (!all_is_finished) {
		all_is_finished = true;
		for (i = 0; i < Nprod * PROD_MULTIPLICATOR; i++) {
			if (share->prod[i]->finished == false) {
				all_is_finished = false;
				break;
			}
		}
	}
	share->all_done = true;
	printf("All producers have finished. Signaling consumers to finish up.\n");
	for (i = 0; i < Ncons; i++)
	{
		rc = pthread_join(cons[i], (void **)&ret);
		printf("main: consumer_%d joined with %d\n", i, *ret);
		free(ret);
	}
	for (i = 0; i < Nprod * PROD_MULTIPLICATOR; i++)
	{
		pthread_mutex_destroy(&share->prod[i]->lock);
		free(share->prod[i]);
	}
	fclose(rfile);
	free(share);
	pthread_exit(NULL);
	exit(0);
}
