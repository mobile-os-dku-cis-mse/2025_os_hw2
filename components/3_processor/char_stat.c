#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "char_stat.h"

void init_stats(CharStats *stats) {
    memset(stats->length_counts, 0, sizeof(stats->length_counts));
    memset(stats->ascii_counts, 0, sizeof(stats->ascii_counts));
    stats->total_line = 0;
}

void update_stats_in_line(char *line, CharStats *stats) {
    size_t length = 0;

    char *sep = "{}()[],;\" \n\t^";
    char *cptr = NULL;
    char *substr = NULL;
    char *brka = NULL;

    cptr = line;

    for (substr = strtok_r(cptr, sep, &brka); substr; substr = strtok_r(NULL, sep, &brka)) {
        length = strlen(substr);
        if (length >= 30) length = 30;

        stats->length_counts[length - 1]++;
        stats->total_line++;
        for (int i = 0; *substr != '\0'; i++) {
            if (*substr < ASCII_SIZE && *substr > 1) {
                stats->ascii_counts[*substr]++;
            }
            substr++;
        }
    }
}

void update_stats_in_chunk(char *chunk, CharStats *stats) {
    if (chunk == NULL || stats == NULL) return;

    char *line = NULL;
    char *brka = NULL;

    for (line = strtok_r(chunk, "\n", &brka); line != NULL; line = strtok_r(NULL, "\n", &brka)) {
        update_stats_in_line(line, stats);
    }
}

void accumulate_stats(CharStats *stats_main, CharStats *stats_thread) {
    stats_main->total_line += stats_thread->total_line;
    for (int i = 0; i < MAX_STRING_LENGTH; i++) {
        stats_main->length_counts[i] += stats_thread->length_counts[i];
    }
    for (int i = 0; i < ASCII_SIZE; i++) {
        stats_main->ascii_counts[i] += stats_thread->ascii_counts[i];
    }
}

void print_stats(CharStats *stats) {
    printf("*** print out distributions *** \n");
    printf("  #ch  freq \n");
    for (int i = 0; i < 30; i++) {
        int j = 0;
        int num_star = stats->length_counts[i] * 80 / stats->total_line;
        printf("[%3d]: %4d \t", i + 1, stats->length_counts[i]);
        for (j = 0; j < num_star; j++)
            printf("*");
        printf("\n");
    }
    printf(
        "       A        B        C        D        E        F        G        H        I        J \n       K        L        M        N        O        P        Q        R        S        T \n       U        V        W        X        Y        Z\n");
    printf(
        "%8d %8d %8d %8d %8d %8d %8d %8d %8d %8d \n%8d %8d %8d %8d %8d %8d %8d %8d %8d %8d \n%8d %8d %8d %8d %8d %8d\n",
        stats->ascii_counts['A'] + stats->ascii_counts['a'],
        stats->ascii_counts['B'] + stats->ascii_counts['b'],
        stats->ascii_counts['C'] + stats->ascii_counts['c'],
        stats->ascii_counts['D'] + stats->ascii_counts['d'],
        stats->ascii_counts['E'] + stats->ascii_counts['e'],
        stats->ascii_counts['F'] + stats->ascii_counts['f'],
        stats->ascii_counts['G'] + stats->ascii_counts['g'],
        stats->ascii_counts['H'] + stats->ascii_counts['h'],
        stats->ascii_counts['I'] + stats->ascii_counts['i'],
        stats->ascii_counts['J'] + stats->ascii_counts['j'],
        stats->ascii_counts['K'] + stats->ascii_counts['k'],
        stats->ascii_counts['L'] + stats->ascii_counts['l'],
        stats->ascii_counts['M'] + stats->ascii_counts['m'],
        stats->ascii_counts['N'] + stats->ascii_counts['n'],
        stats->ascii_counts['O'] + stats->ascii_counts['o'],
        stats->ascii_counts['P'] + stats->ascii_counts['p'],
        stats->ascii_counts['Q'] + stats->ascii_counts['q'],
        stats->ascii_counts['R'] + stats->ascii_counts['r'],
        stats->ascii_counts['S'] + stats->ascii_counts['s'],
        stats->ascii_counts['T'] + stats->ascii_counts['t'],
        stats->ascii_counts['U'] + stats->ascii_counts['u'],
        stats->ascii_counts['V'] + stats->ascii_counts['v'],
        stats->ascii_counts['W'] + stats->ascii_counts['w'],
        stats->ascii_counts['X'] + stats->ascii_counts['x'],
        stats->ascii_counts['Y'] + stats->ascii_counts['y'],
        stats->ascii_counts['Z'] + stats->ascii_counts['z']);
}
