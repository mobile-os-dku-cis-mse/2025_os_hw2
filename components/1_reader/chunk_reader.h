#ifndef INC_2025_OS_HW2_CHUNK_READER_H
#define INC_2025_OS_HW2_CHUNK_READER_H

#include <stdio.h>
#include "../common.h"

typedef struct {
    long start;
    long end;
} ReadRange;

typedef struct {
    FILE *file;
    int fd;
    ReadRange range;
    long current;
    size_t chunk_size;
    int chunk_counter;
} ChunkReader;

void chunk_reader_init(ChunkReader *reader, FILE *file, ReadRange range, size_t chunk_size);
int chunk_reader_has_more(ChunkReader *reader);
DataUnit chunk_reader_next(ChunkReader *reader);
void chunk_reader_destroy(ChunkReader *reader);

#endif