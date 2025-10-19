#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stdatomic.h>

#define MAX_STRING_LENGTH 30
#define ASCII_SIZE 256

atomic_int stat[MAX_STRING_LENGTH];
atomic_int stat2[ASCII_SIZE];

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
	bool stats_computed;
	bool no_print;
} so_t;

void update_stats(char *line)
{
	char *cptr = NULL;
	char *substr = NULL;
	char *brka = NULL;
	char *sep = "{}()[],;\" \n\t^";
	size_t length = 0;

	cptr = line;
	for (substr = strtok_r(cptr, sep, &brka); substr; substr = strtok_r(NULL, sep, &brka))
	{
		length = strlen(substr);
		// update stats

		// length of the sub-string
		if (length >= 30)
			length = 30;
		atomic_fetch_add(&stat[length - 1], 1);

		// number of the character in the sub-string
		for (int i = 0; i < length; i++)
		{
			if (*cptr < 256 && *cptr > 1)
			{
				atomic_fetch_add(&stat2[*cptr], 1);
			}
			cptr++;
		}
		cptr++;
		if (*cptr == '\0')
			break;
	}
}

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
			if (so->prod[id_dataprod]->is_full != true && pthread_mutex_trylock(&so->prod[id_dataprod]->lock) == 0)
			{
				if (so->prod[id_dataprod]->is_full == true)
				{
					pthread_mutex_unlock(&so->prod[id_dataprod]->lock);
					id_dataprod++;
					continue;
				}
				so->prod[id_dataprod]->finished = false;
				so->prod[id_dataprod]->line = strdup(line);
				so->prod[id_dataprod]->is_full = true;
				so->prod[id_dataprod]->actual_linenum = actual_linenum;
				pthread_mutex_unlock(&so->prod[id_dataprod]->lock);
				i++;
				done = true;
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
		if (so->prod[id_dataprod]->is_full != false && pthread_mutex_trylock(&so->prod[id_dataprod]->lock) == 0)
		{
			if (so->prod[id_dataprod]->is_full == false)
			{
				pthread_mutex_unlock(&so->prod[id_dataprod]->lock);
				id_dataprod++;
				continue;
			}
			line = so->prod[id_dataprod]->line;
			if (!so->no_print)
				printf("Cons_%x: [%02d:%02d] %s",
					   (unsigned int)pthread_self(), i, so->linenum, line);
			so->prod[id_dataprod]->line = NULL;
			so->prod[id_dataprod]->is_full = false;
			so->prod[id_dataprod]->finished = true;
			pthread_mutex_unlock(&so->prod[id_dataprod]->lock);
			if (line != NULL)
			{
				if (so->stats_computed)
					update_stats(line);
				free(line);
			}
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
	int sum = 0;

	if (argc == 1)
	{
		printf("usage: ./prod_cons <readfile> #Producer #Consumer\n");
		exit(0);
	}

	so_t *share = malloc(sizeof(so_t));
	memset(share, 0, sizeof(so_t));
	memset(stat, 0, sizeof(stat));
	memset(stat2, 0, sizeof(stat));
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

	if (strcmp(argv[argc - 1], "all") == 0)
	{
		share->stats_computed = true;
	}
	else if (strcmp(argv[argc - 1], "no_print") == 0)
	{
		share->stats_computed = true;
		share->no_print = true;
	}
	else
	{
		share->stats_computed = false;
	}

	share->rfile = rfile;
	share->linenum = 0;
	share->Ndataprod = Nprod + Ncons;
	share->prod = malloc(share->Ndataprod * sizeof(so_producer_t *));
	for (i = 0; i < share->Ndataprod; i++)
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
	while (!all_is_finished)
	{
		all_is_finished = true;
		for (i = 0; i < share->Ndataprod; i++)
		{
			if (share->prod[i]->finished == false)
			{
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
	if (share->stats_computed)
	{
		for (i = 0; i < 30; i++)
		{
			sum += stat[i];
		}
		printf("*** print out distributions *** \n");
		printf("  #ch  freq \n");
		for (i = 0; i < 30; i++)
		{
			int j = 0;
			int num_star = stat[i] * 80 / sum;
			printf("[%3d]: %4d \t", i + 1, stat[i]);
			for (j = 0; j < num_star; j++)
				printf("*");
			printf("\n");
		}
		printf("       A        B        C        D        E        F        G        H        I        J        K        L        M        N        O        P        Q        R        S        T        U        V        W        X        Y        Z\n");
		printf("%8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d %8d\n",
			   stat2['A'] + stat2['a'], stat2['B'] + stat2['b'], stat2['C'] + stat2['c'], stat2['D'] + stat2['d'], stat2['E'] + stat2['e'],
			   stat2['F'] + stat2['f'], stat2['G'] + stat2['g'], stat2['H'] + stat2['h'], stat2['I'] + stat2['i'], stat2['J'] + stat2['j'],
			   stat2['K'] + stat2['k'], stat2['L'] + stat2['l'], stat2['M'] + stat2['m'], stat2['N'] + stat2['n'], stat2['O'] + stat2['o'],
			   stat2['P'] + stat2['p'], stat2['Q'] + stat2['q'], stat2['R'] + stat2['r'], stat2['S'] + stat2['s'], stat2['T'] + stat2['t'],
			   stat2['U'] + stat2['u'], stat2['V'] + stat2['v'], stat2['W'] + stat2['w'], stat2['X'] + stat2['x'], stat2['Y'] + stat2['y'],
			   stat2['Z'] + stat2['z']);
	}
	for (i = 0; i < share->Ndataprod; i++)
	{
		pthread_mutex_destroy(&share->prod[i]->lock);
		free(share->prod[i]);
	}
	free(share->prod);
	fclose(rfile);
	free(share);
	pthread_exit(NULL);
	exit(0);
}
