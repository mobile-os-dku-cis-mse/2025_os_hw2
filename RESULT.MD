# Results for OS Homework 2

## Version 1

### Single Thread for consummer and producer each
- Execution Time (Light file): _0m0,108s_

- Execution Time (Huge file (5 GB)): More than _5m36,636s_ (I stopped it, too long)


### Multiple Threads
- Execution Time with 2 thread each (Light file): _0m0,110s_

- Execution Time with 5 thread each (Light file): _0m0,091s_

- Execution Time with 10 thread each (Light file): _0m0,096s_


- Execution Time with 5 thread each (Huge file (5 GB)): more than _5m41,690s_ (I stopped it, too long)

---

## Version 2

### Single Thread for consummer and producer each
- Execution Time (Light file): _0m0,084s_

*Skipped the huge file test because it will be surely too long*

### Multiple Threads
- Execution Time with 2 thread each (Light file): _0m0,066s_

- Execution Time with 5 thread each (Light file): _0m0,087s_

- Execution Time with 10 thread each (Light file): _0m0,120s_

***Remarks: We are losing time because the file is so light that what takes more time is the threads handling.***

- Execution Time with 5 threads each (**Huge file** (5 GB)): _4m40,969s_

- Execution Time with 5 threads each and statistics (**Huge file** (5 GB)): _5m3,589s_

- Execution Time with 5 threads each and statistics but without printing (**Huge file** (5 GB)): 0m27,052s

- Execution Time with 1 threads each and statistics but without printing (**Huge file** (5 GB)): 0m26,681s

**We see that the threads are here to optimize the IO because there is almost no difference between the *5 threads each* and *1 threads each* tests without printing.**

---
