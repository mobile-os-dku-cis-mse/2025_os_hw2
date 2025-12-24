#define _XOPEN_SOURCE 700
#include "chunk_reader.h"
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

void chunk_reader_init(ChunkReader *reader, FILE *file, ReadRange range, size_t chunk_size) {
    reader->file = file;
    reader->fd = fileno(file);
    reader->range = range;
    reader->current = range.start;
    reader->chunk_size = chunk_size;
    reader->chunk_counter = 0;
}

int chunk_reader_has_more(ChunkReader *reader) {
    return reader->current < reader->range.end;
}

DataUnit chunk_reader_next(ChunkReader *reader) {
    if (!chunk_reader_has_more(reader)) {
        return (DataUnit){.data = NULL, .size = 0, .id = -1};
    }
    
    long remaining = reader->range.end - reader->current;
    size_t read_size = (remaining < (long)reader->chunk_size) ? remaining : reader->chunk_size;
    
    char *buffer = (char*)malloc(read_size);
    if (buffer == NULL) {
        return (DataUnit){.data = NULL, .size = 0, .id = -1};
    }
    
    ssize_t bytes_read = pread(reader->fd, buffer, read_size, reader->current);
    
    if (bytes_read <= 0) {
        free(buffer);
        return (DataUnit){.data = NULL, .size = 0, .id = -1};
    }
    
    reader->current += bytes_read;
    
    DataUnit unit = {
        .data = buffer,
        .size = bytes_read,
        .id = reader->chunk_counter++
    };
    
    return unit;
}

void chunk_reader_destroy(ChunkReader *reader) {
    (void)reader;
}
// pool
//
// ssize_t chunk_reader_read_into(ChunkReader *reader, char *buffer, size_t max_len) {
//     if (!chunk_reader_has_more(reader)) {
//         return 0;
//     }
//
//     long remaining = reader->range.end - reader->current;
//     size_t read_size = (remaining < (long)max_len) ? remaining : max_len;
//
//     if (read_size == 0) return 0;
//
//     ssize_t bytes_read = pread(reader->fd, buffer, read_size, reader->current);
//
//     if (bytes_read > 0) {
//         reader->current += bytes_read;
//     }
//
//     return bytes_read;
// }
