#ifndef INC_2025_OS_HW2_COMMON_H
#define INC_2025_OS_HW2_COMMON_H

#include <pthread.h>
#include <stdbool.h>

typedef struct {
    void *data;
    size_t size;
    int id;
    bool should_free;
} DataUnit;

typedef struct {
    long start;
    long end;
} ReadRange;

#endif