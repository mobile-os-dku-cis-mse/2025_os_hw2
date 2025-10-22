HW2: Multi-threaded Word Count

Student: Javokhirbek Khalikov
Student ID: 32223879
Class: Operating Systems, Dankook University

---

1. Introduction
This program implements a multi-threaded producer-consumer system to read a text file and count the frequency of each alphabet character.

- Producer: reads lines from a file and places them into a shared buffer.
- Consumers: multiple threads take lines from the buffer, count characters, and update global statistics.
- Synchronization is handled using pthread mutexes and condition variables to avoid race conditions.


2. Files
- prod_cons.c        : Main C program with producer and consumer threads.
- sample.txt         : Sample input file.
- prod_cons.exe      : Compiled binary (optional).



3. How to Build
Make sure GCC with pthread support is installed (e.g., MinGW64 on Windows). Then compile:

    gcc -pthread prod_cons.c -o prod_cons

---

4. How to Run
Usage:

    ./prod_cons <filename> <num_consumers>

Example:

    ./prod_cons sample.txt 2

- <filename>      : Text file to read
- <num_consumers> : Number of consumer threads


5. Output
The program prints the count of each alphabet character in the file. Example output:

    Character counts:
    a: 8
    b: 2
    c: 7
    d: 6
    e: 15
    f: 3
    g: 4
    h: 9
    i: 16
    j: 0
    k: 0
    l: 9
    m: 4
    n: 9
    o: 9
    p: 2
    q: 0
    r: 10
    s: 9
    t: 17
    u: 5
    v: 0
    w: 2
    x: 1
    y: 0
    z: 0


6. Notes
- Tested with 1 producer + multiple consumers (1–16 threads).
- Works with files of any size.
- Uses mutexes and condition variables to synchronize access to shared buffer and character statistics.
- Can be easily extended to larger files or more consumer threads for performance testing.
