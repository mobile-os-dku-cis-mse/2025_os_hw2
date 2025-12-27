CC = gcc
CFLAGS = -Wall -Wextra -pthread -O3 -D_XOPEN_SOURCE=700 -I./components

# 공통 모듈
COMMON_SRCS = components/3_processor/char_stat.c components/4_statistics/timer.c

# 통계 모듈
METRICS_SRCS = components/4_statistics/thread_metrics.c

# Readers
READER_LINE  = components/1_reader/line_reader.c
READER_CHUNK = components/1_reader/chunk_reader.c
READER_MMAP  = components/1_reader/mmap_reader.c

# Buffers
BUF_SL    = components/2_buffer/single_line_buffer.c
BUF_SC    = components/2_buffer/single_chunk_buffer.c
BUF_MC    = components/2_buffer/multiple_chunk_buffer.c
BUF_SHARD = components/2_buffer/sharded_buffer.c components/2_buffer/multiple_chunk_buffer.c

# --- [Targets] ---

# 생성할 실행 파일 목록
TARGETS = 01_single_line \
          02_single_chunk \
          03_multiple_chunk \
          04_detailed_profile \
          05_sharded \
          06_mmap \
          07_mmap_sharded_refactored \

all: $(TARGETS)

01_single_line: scenarios/01_single_line.c
	$(CC) $(CFLAGS) -o $@ $< $(READER_LINE) $(BUF_SL) $(COMMON_SRCS)

02_single_chunk: scenarios/02_single_chunk.c
	$(CC) $(CFLAGS) -o $@ $< $(READER_CHUNK) $(BUF_SC) $(COMMON_SRCS)

03_multiple_chunk: scenarios/03_multiple_chunk.c
	$(CC) $(CFLAGS) -o $@ $< $(READER_CHUNK) $(BUF_MC) $(COMMON_SRCS)

04_detailed_profile: scenarios/04_detailed_profile.c
	$(CC) $(CFLAGS) -o $@ $< $(READER_CHUNK) $(BUF_MC) $(METRICS_SRCS) $(COMMON_SRCS)

05_sharded: scenarios/05_sharded.c
	$(CC) $(CFLAGS) -o $@ $< $(READER_CHUNK) $(BUF_SHARD) $(COMMON_SRCS)

06_mmap: scenarios/06_mmap.c
	$(CC) $(CFLAGS) -o $@ $< $(READER_MMAP) $(BUF_MC) $(COMMON_SRCS)

07_mmap_sharded_refactored: scenarios/07_mmap_sharded_refactored.c
	$(CC) $(CFLAGS) -o $@ $< $(READER_MMAP) $(BUF_SHARD) $(COMMON_SRCS)

# 정리 (Clean)
clean:
	rm -f $(TARGETS)

.PHONY: all clean