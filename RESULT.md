# Producer-Consumer Multi-Threaded Program

## Introduction
This program demonstrates the Producer-Consumer problem using POSIX threads (pthread). It reads lines from a file using producer threads and processes them using consumer threads. The program ensures thread safety and data consistency through synchronization primitives like mutex locks and condition variables.

### Key Features
- Supports multiple producers and consumers.
- Implements a circular buffer for efficient data sharing.
- Collects character statistics (a-z) from the input file.
- Measures execution time for performance analysis.

---

## Synchronization Strategy
- Mutex (pthread_mutex_t): Protects shared buffer and global statistics.
- Condition Variables (pthread_cond_t):
  - not_empty: Signals consumers when buffer has data.
  - not_full: Signals producers when buffer has space.

---

## Program Structure
1. **Main Thread**:
   - Parses arguments: `<filename> <#producers> <#consumers>`.
   - Initializes shared buffer and synchronization primitives.
   - Creates producer and consumer threads.
   - Waits for all threads to finish and aggregates statistics.

2. **Producer Threads**:
   - Read lines from the file.
   - Insert lines into the circular buffer.
   - Signal consumers when data is available.

3. **Consumer Threads**:
   - Remove lines from the buffer.
   - Print lines and update local character statistics.
   - Merge statistics into global stats at the end.

---

## Build Instructions
```bash
gcc -pthread prod_cons_modified.c -o prod_cons
```

## Run Examples
```bash
./prod_cons sample.txt 1 2   # 1 producer, 2 consumers
./prod_cons sample.txt 1 4   # 1 producer, 4 consumers
./prod_cons sample.txt 2 3   # 2 producers, 3 consumers
```

---

## Output Example
```
Main: Producer 0 joined with 780
Main: Consumer 0 joined with 780
Execution time: 0.0075 seconds

Character Statistics:
a: 2379
b: 546
c: 2067
d: 1092
e: 3627
f: 585
g: 546
h: 780
i: 2379
j: 39
k: 156
l: 1287
m: 1053
n: 2262
o: 2106
p: 1053
q: 39
r: 2613
s: 2613
t: 2379
u: 1209
v: 429
w: 312
x: 195
y: 546
z: 156

---

## Performance Comparison Template
used the sample.txt
| Producers | Consumers | Execution Time (s) |
|-----------|-----------|----------------------|
| 1         | 1         | 0.0075               |
| 1         | 2         | 0.0088               |
| 1         | 3         | 0.0083               |
| 1         | 4         | 0.0097               |
| 1         | 5         | 0.0095               |
| 1         | 10        | 0.0101               |
| 2         | 1         | 0.0086               |
| 2         | 2         | 0.0097               |
| 2         | 3         | 0.0091               |
| 2         | 4         | 0.0100               |
| 2         | 5         | 0.0094               |
| 2         | 10        | 0.0112               |

Performance with the sample_big.txt
Preducers: 1      Consumers: 1      Execution time (s): 1373.6975 s


