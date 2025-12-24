#include "cache.h"
#include <stdio.h>
#include <linux/perf_event.h>
#include <sys/ioctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <string.h>
static long perf_event_open(struct perf_event_attr *hw_event, pid_t pid,
                            int cpu, int group_fd, unsigned long flags) {
    return syscall(__NR_perf_event_open, hw_event, pid, cpu, group_fd, flags);
}

int init_cache_counters(CachePerfCounter *counter) {
    struct perf_event_attr pe;

    memset(&pe, 0, sizeof(struct perf_event_attr));
    pe.type = PERF_TYPE_HARDWARE;
    pe.size = sizeof(struct perf_event_attr);
    pe.config = PERF_COUNT_HW_CACHE_REFERENCES;
    pe.disabled = 1;
    pe.exclude_kernel = 1;

    counter->fd_refs = perf_event_open(&pe, 0, -1, -1, 0);
    if (counter->fd_refs == -1) {
        perror("perf_event_open (cache-references)");
        return -1;
    }

    pe.config = PERF_COUNT_HW_CACHE_MISSES;
    counter->fd_misses = perf_event_open(&pe, 0, -1, -1, 0);
    if (counter->fd_misses == -1) {
        perror("perf_event_open (cache-misses)");
        close(counter->fd_refs);
        return -1;
    }

    return 0;
}

void start_cache_counters(CachePerfCounter *counter) {
    ioctl(counter->fd_refs, PERF_EVENT_IOC_RESET, 0);
    ioctl(counter->fd_misses, PERF_EVENT_IOC_RESET, 0);
    ioctl(counter->fd_refs, PERF_EVENT_IOC_ENABLE, 0);
    ioctl(counter->fd_misses, PERF_EVENT_IOC_ENABLE, 0);
}

void stop_cache_counters(CachePerfCounter *counter,
                         long long *refs, long long *misses) {
    ioctl(counter->fd_refs, PERF_EVENT_IOC_DISABLE, 0);
    ioctl(counter->fd_misses, PERF_EVENT_IOC_DISABLE, 0);

    read(counter->fd_refs, refs, sizeof(long long));
    read(counter->fd_misses, misses, sizeof(long long));
}

void close_cache_counters(CachePerfCounter *counter) {
    close(counter->fd_refs);
    close(counter->fd_misses);
}
