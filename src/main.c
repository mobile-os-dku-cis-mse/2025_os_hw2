#include "word_count.h"


static void *producer_thread(void *arg) {
    producer_arg_t *pa = (producer_arg_t*)arg;
    queue_t *q = pa->q;
    char *line = NULL;
    size_t len = 0;

    while (true) {
        ssize_t r = getline(&line, &len, pa->fp);
        if (r == -1)
            break;
        char *copy = strdup(line);
        if (!copy) {
            perror("strdup");
            free(line);
            exit(EXIT_FAILURE);
        }
        queue_push(q, copy);
        pa->lines_produced++;
    }
    free(line);

    pthread_mutex_lock(&q->m);
    q->done = true;
    pthread_cond_broadcast(&q->not_empty);
    pthread_mutex_unlock(&q->m);
    return NULL;
}

static double elapsed_ms(struct timespec a, struct timespec b) {
    long sec  = b.tv_sec - a.tv_sec;
    long nsec = b.tv_nsec - a.tv_nsec;
    return (double)sec * 1000.0 + (double)nsec / 1e6;
}

static void print_stats(uint64_t agg[26]) { // Use AI for this display part.
    printf("\nA..Z totals:\n");
    for (int i = 0; i < 26; ++i) {
        printf("%c: %" PRIu64 "%s", 'A'+i, agg[i], (i==25? "\n" : ( (i%6==5) ? "\n" : "\t")));
    }

    printf("\n       A        B        C        D        E        F        G        H        I        J        K        L        M        N        O        P        Q        R        S        T        U        V        W        X        Y        Z\n");
    printf("%8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 " %8" PRIu64 "\n",
        agg[0], agg[1], agg[2], agg[3], agg[4], agg[5], agg[6], agg[7], agg[8], agg[9], agg[10], agg[11], agg[12], agg[13], agg[14], agg[15], agg[16], agg[17], agg[18], agg[19], agg[20], agg[21], agg[22], agg[23], agg[24], agg[25]);
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "usage: %s <file> <#producers> <#consumers> [buffer_capacity]\n", argv[0]);
        return EXIT_FAILURE;
    }

    const char *path = argv[1];
    int nprod = atoi(argv[2]);
    int ncons = atoi(argv[3]);
    int cap   = (argc >= 5) ? atoi(argv[4]) : 1024;
    if (cap < 1) cap = 1024;

    if (nprod < 1) nprod = 1;
    if (ncons < 1) ncons = 1;

    if (nprod > 1) {
        fprintf(stderr, "[info] This assignment want 1 producer. Forcing producers=1 (passed=%d).\n", nprod);
        nprod = 1;
    }

    FILE *fp = fopen(path, "rb");
    if (!fp) { perror(path); return EXIT_FAILURE; }

    queue_t q;
    queue_init(&q, cap);

    pthread_t prod_tid;
    producer_arg_t pa = { .q = &q, .fp = fp, .lines_produced = 0 };

    consumer_arg_t *cons = (consumer_arg_t*)calloc(ncons, sizeof(consumer_arg_t));
    pthread_t *cons_tids = (pthread_t*)calloc(ncons, sizeof(pthread_t));

    if (!cons || !cons_tids) {
        perror("calloc");
        free(cons_tids);
        free(cons);
        return EXIT_FAILURE;
    }

    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    if (pthread_create(&prod_tid, NULL, producer_thread, &pa) != 0) {
        perror("pthread_create(producer)");
        free(cons_tids);
        free(cons);
        return EXIT_FAILURE;
    }
    for (int i = 0; i < ncons; ++i) {
        cons[i].q = &q;
        cons[i].lines_consumed = 0;
        if (pthread_create(&cons_tids[i], NULL, consumer_thread, &cons[i]) != 0) {
            perror("pthread_create(consumer)");
            free(cons_tids);
            free(cons);
            return EXIT_FAILURE;
        }
    }

    for (int i = 0; i < ncons; ++i)
        pthread_join(cons_tids[i], NULL);
    pthread_join(prod_tid, NULL);

    clock_gettime(CLOCK_MONOTONIC, &t1);

    uint64_t agg[26] = {0};
    long total_lines = 0;
    for (int i = 0; i < ncons; ++i) {
        for (int j = 0; j < 26; ++j) agg[j] += cons[i].counter[j];
        total_lines += cons[i].lines_consumed;
    }

    printf("Producers: %d (effective: 1) | Consumers: %d | Buffer cap: %d\n", atoi(argv[2]), ncons, cap);
    printf("Lines produced: %ld | Lines consumed: %ld\n", pa.lines_produced, total_lines);
    printf("Elapsed: %.3f ms\n", elapsed_ms(t0, t1));
    print_stats(agg);

    queue_destroy(&q);
    fclose(fp);
    free(cons);
    free(cons_tids);
    return EXIT_SUCCESS;
}
