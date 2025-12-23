#ifndef INC_2025_OS_HW2_MMAP_READER_H
#define INC_2025_OS_HW2_MMAP_READER_H

#include <stdio.h>
#include "../common.h"

typedef struct {
    char *map_addr;
    size_t map_len;
    long mapped_offset;

    ReadRange range;
    long current;
    size_t chunk_size;
    int chunk_counter;
} MmapReader;

void mmap_reader_init(MmapReader *reader, FILE *file, ReadRange range, size_t chunk_size);
int mmap_reader_has_more(MmapReader *reader);
DataUnit mmap_reader_next(MmapReader *reader);
void mmap_reader_destroy(MmapReader *reader);

#endif