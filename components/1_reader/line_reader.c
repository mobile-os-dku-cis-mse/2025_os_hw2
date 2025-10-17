#define _XOPEN_SOURCE 700
#include "line_reader.h"
#include <stdlib.h>
#include <string.h>

void line_reader_init(LineReader *reader, FILE *file, ReadRange range) {
    reader->file = file;
    reader->range = range;
    reader->current = range.start;
    reader->line_counter = 0;
    
    fseek(file, range.start, SEEK_SET);
}

int line_reader_has_more(LineReader *reader) {
    return reader->current < reader->range.end && !feof(reader->file);
}

DataUnit line_reader_next(LineReader *reader) {
    if (!line_reader_has_more(reader)) {
        return (DataUnit){.data = NULL, .size = 0, .id = -1};
    }
    
    char *line = NULL;
    size_t len = 0;
    ssize_t read = getline(&line, &len, reader->file);
    
    if (read == -1) {
        if (line) free(line);
        return (DataUnit){.data = NULL, .size = 0, .id = -1};
    }
    
    reader->current = ftell(reader->file);
    
    DataUnit unit = {
        .data = line,
        .size = read,
        .id = reader->line_counter++
    };
    
    return unit;
}

void line_reader_destroy(LineReader *reader) {
    (void)reader;
}