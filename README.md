# Multi-thread word count
This project implements a multi-thread producer-consumer system in C. The goal is to learn thread creation, synchronization using mutex and condition variables, and shared buffer management. The program also counts the occurences of each alphabet character in a given file.

## Feature
- Multi-threaded producer-consumer implementation using POSIX thread (pthread).
- Shared buffer synchronization with mutex and condition variables.
- Letter frequenct counting with thread-safe global storage.
- Performance measurement of execution time with different numbers of producers, consumers, and buffer sizes.

## Project Structure
```
project-root/
├── Code/
│ ├── prod_cons.c
│ ├── pthread.c
│ ├── char_stat.c
│ └── prod_cons.h
├── README.md
├── LICENSE
└── Assignment2 - Multi-threaded word count Document (유준혁, 32212808, Department of MSE)
```

## Installation

1. Clone the repository
	```
	git clone [Link]
	```

2. Navigate to the project directory
	```
	cd code
	```

## Usage
1. Build the project
	```
	make
	```

2. Run the program
	```
	./prod_cons [readfile] [producer] [consumer] [buf_size]
	```

## Test
- Measure execution time with different numbers of producers, consumers, and buffer sizes.
- Verify letter counts match expected results for input files.
- Check that no deadlocks or race conditions occur even under extreme settings.

## Contributing
1. Fork this repository.

2. Create a new branch.
	```
	git checkout -b feature/YourFeature
	```

3. Commit your changes.
	```
	git commit -m "Add some feature"
	```

4. Push to the branch.
	```
	git push origin feature/YourFeature
	```

5. Open a pull request.

## License
This project is licensed under the MIT License.

## Authors
- Yoo JunHyuk ([@YooJunHyuk123](https://github.com/YooJunHyuk123))

- Email: yjh32212808@dankook.ac.kr