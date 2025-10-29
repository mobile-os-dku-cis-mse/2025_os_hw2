#ifndef SHARDED_BUFFER_H
#define SHARDED_BUFFER_H

#include "multiple_chunk_buffer.h"

typedef struct {
    MultiChunkBuffer **shards;
    int n_shards;
    int finished;
    int active_producers;
    pthread_mutex_t global_lock;
} ShardedBuffer;

void sb_init(ShardedBuffer *sb, int n_shards, int capacity_per_shard, int n_prod);
void sb_destroy(ShardedBuffer *sb);

void sb_put(ShardedBuffer *sb, DataUnit unit, int producer_id);

DataUnit sb_get(ShardedBuffer *sb, int consumer_id);

void sb_notify_producer_finished(ShardedBuffer *sb);

#endif