#include "word_count.h"

void *consumer_thread(void *arg) {
    consumer_arg_t *ca = (consumer_arg_t*)arg;
    memset(ca->counter, 0, sizeof(ca->counter));

    char *line = NULL;
    while (queue_pop(ca->q, &line)) {
        for (char *p = line; *p; ++p) {
            unsigned char c = (unsigned char)*p;
            if (isalpha(c)) {
                c = (unsigned char)toupper(c);
                ca->counter[c - 'A']++;
            }
        }
        ca->lines_consumed++;
        free(line);
    }
    return NULL;
}
