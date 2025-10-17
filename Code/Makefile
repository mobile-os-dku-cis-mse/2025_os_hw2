COMPILER = gcc
OPTION = -Wall -g -pthread
PROGRAM = word_count
FILE = char_stat.o prod_cons.o pthread.o

all: $(PROGRAM)

$(PROGRAM): $(FILE)
	$(COMPILER) $(OPTION) -o $(PROGRAM) $(FILE)

%.o: %.c
	$(COMPILER) $(OPTION) -c $< -o $@

clear:
	rm -f $(FILE) $(PROGRAM)