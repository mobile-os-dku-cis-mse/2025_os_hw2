#ifndef CHAR_STAT_H
#define CHAR_STAT_H

#include <stddef.h>

#define ASCII_SIZE 256
#define MAX_STRING_LENGTH 30

typedef struct {
    int  length_counts[MAX_STRING_LENGTH];
    int  ascii_counts[ASCII_SIZE];
    long total_line;
} CharStats;

void init_stats(CharStats *stats);
void update_stats_in_line(char *line, CharStats *stats);
void update_stats_in_chunk(char *chunk, size_t size, CharStats *stats);
void accumulate_stats(CharStats *stats_main, CharStats *stats_thread);
void print_stats(CharStats *stats);

#endif