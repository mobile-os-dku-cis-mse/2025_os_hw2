#ifndef INC_2025_OS_HW2_PROD_CONS_H
    #define _GNU_SOURCE
    #include <pthread.h>
    #include <ctype.h>
    #include <errno.h>
    #include <inttypes.h>
    #include <stdio.h>
    #include <stdlib.h>
    #include <string.h>
    #include <time.h>
    #include <stdbool.h>

    // ----------------- QUEUE --------------- //
    typedef struct queue_s{
        char **buf;
        int    cap;
        int    head;
        int    tail;
        int    count;
        int    done;
        pthread_mutex_t m;
        pthread_cond_t  not_empty;
        pthread_cond_t  not_full;
    } queue_t;

    void queue_init(queue_t *q, int cap);
    void queue_destroy(queue_t *q);
    void queue_push(queue_t *q, char *line);
    int queue_pop(queue_t *q, char **out_line);

    // ----------------- PRODUCER --------------- //
    typedef struct producer_arg_s {
        queue_t *q;
        FILE    *fp;
        long     lines_produced;
    } producer_arg_t;

    // ----------------- CONSUMER --------------- //
    typedef struct consumer_arg_s{
        queue_t *q;
        long     lines_consumed;
        uint64_t counter[26];
    } consumer_arg_t;

    void *consumer_thread(void *arg);


#define INC_2025_OS_HW2_PROD_CONS_H

#endif //INC_2025_OS_HW2_PROD_CONS_H