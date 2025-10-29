#include "sharded_buffer.h"
#include <stdlib.h>

void sb_init(ShardedBuffer *sb, int n_shards, int capacity_per_shard, int n_prod) {
    sb->n_shards = n_shards;
    sb->shards = malloc(sizeof(MultiChunkBuffer*) * n_shards);
    sb->active_producers = n_prod;
    sb->finished = 0;
    pthread_mutex_init(&sb->global_lock, NULL);

    for (int i = 0; i < n_shards; i++) {
        sb->shards[i] = malloc(sizeof(MultiChunkBuffer));
        mcb_init(sb->shards[i], 1, capacity_per_shard);
    }
}

void sb_destroy(ShardedBuffer *sb) {
    for (int i = 0; i < sb->n_shards; i++) {
        mcb_destroy(sb->shards[i]);
        free(sb->shards[i]);
    }
    free(sb->shards);
    pthread_mutex_destroy(&sb->global_lock);
}

void sb_put(ShardedBuffer *sb, DataUnit unit, int producer_id) {
    int shard_idx = producer_id % sb->n_shards;
    mcb_put(sb->shards[shard_idx], unit);
}

DataUnit sb_get(ShardedBuffer *sb, int consumer_id) {
    DataUnit unit;
    int start_idx = consumer_id % sb->n_shards;

    while (1) {
        for (int i = 0; i < sb->n_shards; i++) {
            int idx = (start_idx + i) % sb->n_shards;
            unit = mcb_try_get(sb->shards[idx]);
            if (unit.id != -2) {
                return unit;
            }
        }

        pthread_mutex_lock(&sb->global_lock);
        if (sb->finished) {
            int all_empty = 1;
            for(int i=0; i<sb->n_shards; i++) {
                if(sb->shards[i]->count > 0) {
                    all_empty = 0;
                    break;
                }
            }
            if(all_empty) {
                pthread_mutex_unlock(&sb->global_lock);
                return (DataUnit){NULL, 0, -1};
            }
        }
        pthread_mutex_unlock(&sb->global_lock);

        unit = mcb_get(sb->shards[start_idx]);

        if (unit.data != NULL) {
            return unit;
        }

        if (unit.data == NULL) {
            continue;
        }
    }
}

void sb_notify_producer_finished(ShardedBuffer *sb) {
    pthread_mutex_lock(&sb->global_lock);
    sb->active_producers--;
    if (sb->active_producers == 0) {
        sb->finished = 1;
        for (int i = 0; i < sb->n_shards; i++) {
            mcb_notify_producer_finished(sb->shards[i]);
        }
    }
    pthread_mutex_unlock(&sb->global_lock);
}