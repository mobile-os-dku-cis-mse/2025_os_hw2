# os_hw2 : Report

## 1. Introduction

The goal of this homework is to implement a `multi-threaded producer-consumer` program in C using pthreads, mutexes and condition variables.    
    
It reads line from a text file and computes statistics about word lengths and character frequencies.    
    
The program can scale with multiple producers (reading lines from the files) and multiple consumers (processing and analyzing the lines).    
    
## 2. Implementation Details
### Shared Memory Object :
- All producers and consumers share a single instance of `so_t`
  - This struct holds :
    - The shared buffer
    - Mutexes and condition variables for synchronization
    - Shared file pointer
  - Synchronization is achieved bia `file_lock` (ensure only one producer reads from the file at a time) and `lock` (control access to the shared buffer)
### Producer Threads :
- Each producer :
  - Locks `file_lock`
  - Reads a line from the file using `getdelim()`
  - Unlocks `file_lock`
  - Waits for space in the buffer
  - Inserts the line into the buffer
  - Signals consumers
  - When the file ends, it decrements `active_producer`
When all producers are done, the shared flag `done = 1` signals consumers to exit.
### Consumer Threads :
- Each consumer :
  - Wait for available lines (`cond_empty`)
  - Removes a line from the buffer
  - Prints the line with its line number and thread ID
  - Tokenizes it and updates global statistics
  - Signals producers that space is available

## 3. Build Instructions
#### Create a build folder and navigate to it
```shell
mkdir build && cd build
```
#### Run cmake
```shell
cmake ..
```
#### Compile the project
```shell
make
```
#### Run the program
```shell
./word_count <filename> [producers_number] [consumers_number]
```
#### Example
```shell
./word_count test_file 2 2
```

## 3. Working examples
### Test File :
```
This is a test file
With 2 lines
And 3
And 4

```

### Command :
```shell
./word_count test_file 1 2
```

### Output :
```
Prod_9ff6c0: 4 lines
main continuing
Cons_1fe6c0: [00:00] This is a test file
Cons_f77ff6c0: [00:01] With 2 lines
Cons_1fe6c0: [01:02] And 3
Cons_f77ff6c0: [01:03] And 4
Cons_1fe6c0: 2 lines
main: consumer_0 joined with 2
Cons_f77ff6c0: 2 lines
main: consumer_1 joined with 2
main: producer_0 joined with 4

*** print out distributions *** 
  #ch  freq
[  1]:    4     **************************
[  2]:    1     ******
[  3]:    2     *************
[  4]:    4     **************************
[  5]:    1     ******
[  6]:    0     
[  7]:    0     
[  8]:    0     
[  9]:    0     
[ 10]:    0     
[ 11]:    0     
[ 12]:    0     
[ 13]:    0     
[ 14]:    0     
[ 15]:    0     
[ 16]:    0     
[ 17]:    0     
[ 18]:    0     
[ 19]:    0     
[ 20]:    0     
[ 21]:    0     
[ 22]:    0     
[ 23]:    0     
[ 24]:    0     
[ 25]:    0     
[ 26]:    0     
[ 27]:    0     
[ 28]:    0     
[ 29]:    0     
[ 30]:    0     

       A        B        C        D        E        F        G        H        I        J        K        L        M        N        O        P        Q        R        S        T        U        V        W        X        Y        Z
       3        0        0        2        3        1        0        2        5        0        0        2        0        3        0        0        0        0        4        4        0        0        1        0        0        0
```