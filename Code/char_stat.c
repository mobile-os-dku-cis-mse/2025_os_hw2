// char_stat.c
#include <stdio.h>
#include <stdlib.h>
#include "prod_cons.h"

void print_char_stat(long count[26]){
    printf("\n―――――――――― Number of each alphabet character in the file ――――――――――\n");
    for(int i = 0; i < 26; ++i){
        char c = 'A' + i;
        printf("ㆍ%c: %8ld\n", c, count[i]);
    }
    long total = 0;
    for(int i = 0; i < 26; ++i){
        total += count[i];
    }
    printf("ㆍTotal counted letters: %ld\n", total);
}