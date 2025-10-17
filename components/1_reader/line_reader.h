#ifndef INC_2025_OS_HW2_LINE_READER_H
#define INC_2025_OS_HW2_LINE_READER_H

#include <stdio.h>
#include "../common.h"

typedef struct {
    FILE *file;
    ReadRange range;
    long current;
    int line_counter;
} LineReader;

void line_reader_init(LineReader *reader, FILE *file, ReadRange range);
int line_reader_has_more(LineReader *reader);
DataUnit line_reader_next(LineReader *reader);
void line_reader_destroy(LineReader *reader);

#endif