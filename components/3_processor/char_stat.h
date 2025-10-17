#ifndef INC_2025_OS_HW2_CHAR_STAT_H
#define INC_2025_OS_HW2_CHAR_STAT_H

#define MAX_STRING_LENGTH 30
#define ASCII_SIZE  256

typedef struct {
    int length_counts[MAX_STRING_LENGTH];
    int ascii_counts[ASCII_SIZE];
    long total_line;
} CharStats;

void init_stats(CharStats *stats);
void update_stats_in_line(char *line, CharStats *stats);
void update_stats_in_chunk(char *chunk, CharStats *stats);
void accumulate_stats(CharStats *stats_main, CharStats *stats_thread);
void print_stats(CharStats *stats);

#endif