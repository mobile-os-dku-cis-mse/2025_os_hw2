#ifndef INC_2025_OS_HW2_CHUNK_READER_H
#define INC_2025_OS_HW2_CHUNK_READER_H

#include <stdio.h>
#include "../common.h"

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

// 마지막에 추가
ssize_t chunk_reader_read_into(ChunkReader *reader, char *buffer, size_t max_len);
#endif