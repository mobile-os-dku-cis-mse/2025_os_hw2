#define _XOPEN_SOURCE 700
#include "mmap_reader.h"
#include <sys/mman.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>

void mmap_reader_init(MmapReader *reader, FILE *file, ReadRange range, size_t chunk_size) {
    int fd = fileno(file);
    long page_size = sysconf(_SC_PAGE_SIZE);

    long pa_offset = range.start & ~(page_size - 1);

    size_t length = range.end - pa_offset;

    char *addr = mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE, fd, pa_offset);
    if (addr == MAP_FAILED) {
        perror("mmap failed");
        reader->map_addr = NULL;
        reader->map_len = 0;
        return;
    }

    posix_madvise(addr, length, POSIX_MADV_SEQUENTIAL);

    reader->map_addr = addr;
    reader->map_len = length;
    reader->mapped_offset = pa_offset;

    reader->range = range;
    reader->current = range.start;
    reader->chunk_size = chunk_size;
    reader->chunk_counter = 0;
}

int mmap_reader_has_more(MmapReader *reader) {
    if (reader->map_addr == NULL) return 0;
    return reader->current < reader->range.end;
}

DataUnit mmap_reader_next(MmapReader *reader) {
    if (!mmap_reader_has_more(reader)) {
        return (DataUnit){.data = NULL, .size = 0, .id = -1};
    }

    long remaining = reader->range.end - reader->current;
    size_t read_size = (remaining < (long)reader->chunk_size) ? remaining : reader->chunk_size;

    long offset_in_map = reader->current - reader->mapped_offset;
    char *data_ptr = reader->map_addr + offset_in_map;

    reader->current += read_size;

    DataUnit unit = {
        .data = data_ptr,
        .size = read_size,
        .id = reader->chunk_counter++,
        .should_free = false
    };

    return unit;
}

void mmap_reader_destroy(MmapReader *reader) {
    if (reader->map_addr && reader->map_addr != MAP_FAILED) {
        munmap(reader->map_addr, reader->map_len);
    }
}