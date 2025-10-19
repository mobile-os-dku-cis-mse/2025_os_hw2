# HW2 — Multithreaded Producer/Consumer & Letter Count

## 1) Overview
This program demonstrates a **producer/consumer** pattern in C with **pthreads**.
Goal: read a text file **line by line** (producer) and process the lines in **parallel**
(consumers) to **count the frequency of letters A..Z** (case-insensitive).

Why? It practices **synchronization** (mutex + condition variables), **concurrency**,
and the design of a **bounded queue**.

---

## 2) Features
- One **producer** reads lines from a `FILE*` and enqueues them into a **circular bounded queue**.
- **N consumers** dequeue lines and count letter frequencies (A..Z) in parallel.
- **Per-thread local counters**, then a **single merge** into the global counter (low contention).
- **Wall-time** measurement to compare performance for different thread counts.
- **Optional line printing** (disable with `print_lines=0`).

---

## 3) Build & Run

### 3.1 Requirements
- GCC/Clang on a POSIX system (Linux/macOS)
- Pthreads (`-pthread`)

### 3.2 Build
```bash
make
```
This produces `./prod_cons_fixed` (and `./prod_cons` if the original source is present).

### 3.3 Run
```bash
./prod_cons_fixed <file_path> <#producers> <#consumers>
```
> **Note:** only **1 producer** is supported (simple, correct I/O semantics).
> Values `#producers > 1` are ignored gracefully.

Examples:
```bash
# Baseline: 1 consumer
./prod_cons_fixed ./sample.txt 1 1

# Parallel: 4 consumers
./prod_cons_fixed ./sample.txt 1 4
```

Useful parameters (via `Makefile`):
- `QUEUE_CAP`: queue capacity (default: 1024)
  ```bash
  make QUEUE_CAP=4096 && ./prod_cons_fixed ./sample.txt 1 8
  ```

---

## 4) Architecture & Shared State

### 4.1 High-level picture
```
      +---------------------+
      |      Producer       |
      |    (read lines)     |
      +----------+----------+
                 |
                 |  push (char*)
                 v
       +---------+----------+
       |  Bounded ring buf  |  <= mutex + conds (not_empty / not_full)
       +---------+----------+
                 ^
       pop()     |      pop()      pop()
         |       |        |          |
     +---+---+ +---+---+ +---+---+ +---+---+
     | Cons 0| | Cons 1| | Cons 2| | Cons k|  (N consumers)
     +---+---+ +---+---+ +---+---+ +---+---+
         |         |         |          |
      local A..Z  local A..Z ...     local A..Z
         \__________   ________/________/
                    \ /
                global A..Z   <= single merge per consumer
```

### 4.2 Main structures
- `line_queue_t`: circular queue of `char*` with
  - `buf[cap]`, `head`, `tail`, `count`
  - `done`: producer finished
  - `pthread_mutex_t m`, `pthread_cond_t not_empty`, `pthread_cond_t not_full`
- `shared_t`: global shared state
  - `FILE *rfile`, `line_queue_t queue`
  - `unsigned long global_alpha[26]`, `pthread_mutex_t global_lock`
  - `int print_lines`

---

## 5) Synchronization (mutex + cond vars)

### 5.1 Queue invariants
- `0 <= count <= cap`
- `head`/`tail` advance **circularly**: `idx = (idx + 1) % cap`
- `count==0` ⇒ empty / `count==cap` ⇒ full

### 5.2 Producer: `q_push`
```c
lock(m);
while (count == cap) wait(not_full, m);

buf[tail] = line;
tail = (tail + 1) % cap;
count++;

signal(not_empty);
unlock(m);
```

Finishing up:
```c
lock(m);
done = 1;
broadcast(not_empty);  // wake up any waiting consumers
unlock(m);
```

### 5.3 Consumer: `q_pop`
```c
lock(m);
while (count == 0 && !done) wait(not_empty, m);

if (count == 0 && done) { unlock(m); return 0; } // clean shutdown

*out = buf[head];
buf[head] = NULL;
head = (head + 1) % cap;
count--;

signal(not_full);
unlock(m);
return 1;
```

**Important:** termination only when `done==1` **and** `count==0`.

---

## 6) Letter counting (A..Z)

### 6.1 Strategy
- Each consumer keeps a **local** `uint64_t[26]` throughout its run.
- At the end, it **merges once** into `global_alpha[26]` under `global_lock`.
- Benefit: avoids locking the global array per character → **much lower contention**.

### 6.2 Details
- Iterate characters; for each `isalpha(c)`, compute `idx = tolower(c) - 'a'`.
- If `0<=idx<26`, increment `local[idx]`.
- Final output prints A..Z counts and a summary row.

---

## 7) Memory safety & ownership
- The producer **duplicates** each line (`malloc`) and **pushes** it into the queue.
- The consumer that **pops** the line becomes its **owner** and **frees** it after processing.
- No more **double‑free** or **use‑after‑free**: each line has a single owner at a time.
- Cleanup: `fclose(rfile)`, `q_destroy`, `pthread_mutex_destroy`, free thread/arg arrays.

---

## 8) Performance measurement
- Wall-time is measured around thread lifetime with `clock_gettime(CLOCK_MONOTONIC)`.
- Tips:
  - **Disable printing** (`print_lines=0`) for realistic numbers.
  - Try `#consumers` = `1,2,4,8,...`
  - Inspect threads with `htop`.
  - Adjust `QUEUE_CAP` to reduce blocking or memory use.

Example:
```bash
for k in 1 2 4 8; do
  ./prod_cons_fixed ./big_file.txt 1 $k
done
```

---


## 9) Files
- `prod_cons_fixed.c` — full code (queue, threads, counters, timing)
- `Makefile` — build, options, convenient targets (`run`, `clean`)
- `README_HW2.md` — quickstart guide
- *(optional: original `prod_cons.c` for comparison)*

---

## 10) Handy commands
```bash
# build
make

# run (example)
./prod_cons_fixed ./sample.txt 1 4

# larger queue
make QUEUE_CAP=4096
./prod_cons_fixed ./big.txt 1 8

# cleanup
make clean
```
