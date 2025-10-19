# OS Homework 2 — Producer/Consumer Program

## 🧩 Overview

This project implements a **Producer/Consumer system** in C using **POSIX threads (pthreads)** and **mutexes** 
The goal is to process data from a file using multiple producers and consumers, while synchronizing them efficiently.

This work is part of the **Operating Systems** course (Homework 2).

---

## ⚙️ How to Run

To compile the program:

```bash
gcc prod_conv_v2.c -o prod_conv_v2 -lpthread  
```

Then, to execute it:

```bash
./prod_conv_v2 [filename] [nb_producers] [nb_consumers] [mode]  
```

### 🔸 Arguments

| Argument | Description |
|-----------|-------------|
| **1st argument** | **Input filename** to be processed *(mandatory)* |
| **2nd argument** | Number of producer threads *(optional, default = 1)* |
| **3rd argument** | Number of consumer threads *(optional, default = 1)* |
| **4th argument (optional)** | Output mode (see below) |

### 🔸 Optional last argument

| Argument | Description |
|-----------|-------------|
| `all` | Displays both file outputs and detailed statistics |
| `no_print` | Displays only statistics, no file output |
| *(nothing)* | Displays only the file output |

🧠 Example usages:

```bash  
./prod_conv_v2 input.txt  
./prod_conv_v2 input.txt 2 5  
./prod_conv_v2 input.txt 5 5 all  
./prod_conv_v2 input.txt no_print  
```

---

## 🧪 Experimental Results

### Version 1 (Baseline)
The first version of the program was functional but not optimized for large files.  
While performance was acceptable for small files, it became **extremely slow on large inputs (5 GB)** due to inefficient thread and I/O handling.

### Version 2 (Optimized)
The second version focused on improving synchronization and reducing unnecessary waits between threads.  
For small files, execution times became almost instantaneous, and for large files, there was a **notable improvement** compared to Version 1.

However, the results show that:
- For **light files**, the **thread management overhead** dominates — adding more threads doesn’t help much.
- For **large files**, the performance is **mainly I/O-bound**.  
  Increasing the number of threads provides limited gains, since disk I/O becomes the bottleneck.

---

## 📊 Interpretation

- **Single-thread vs multi-thread:**  
  Multithreading doesn’t significantly improve speed for small data due to thread creation and synchronization overhead.

- **I/O-bound operations:**  
  The main limitation is the **file reading/writing speed**, not CPU computation. Even with multiple threads, the gain is minimal once the disk is saturated.

- **Effect of print statements:**  
  Printing to the console slows down the program drastically.  
  When the printing is disabled (`no_print` mode), the same large file (5 GB) is processed in **~27 seconds**, compared to **~5 minutes** with output enabled.  
  This clearly shows that **I/O to the terminal** is much slower than memory or disk operations.

---

## 🧠 Conclusion

Version 2 provides **a much more efficient and cleaner multithreaded design**, especially when managing large data with minimized I/O.  
While the system remains limited by file access speed, the use of `pthread_mutex_t` ensures robust and synchronized operation between producers and consumers.

**Key takeaway:**  
> Optimize I/O operations first — thread parallelism can’t overcome a slow output stream.

---

## 🧰 Technical Notes

- **Language:** C (POSIX standard)
- **Threads:** `pthread_create`, `pthread_join`
- **Synchronization:** `pthread_mutex_t`, `pthread_cond_t`
- **Compilation:** `gcc -lpthread`
- **Platform:** Linux

---

✍️ *Author: Kylian Labrador*  
🧠 *Course: Operating Systems*
