CC = gcc
CFLAGS = -Wall -g -pthread
SRC = prod_cons.c
TARGET = prod_cons
all: $(TARGET)
$(TARGET): $(SRC)
		$(CC) $(CFLAGS) $(SRC) -o $(TARGET)
run: $(TARGET)
		./$(TARGET) prod_cons.c 3 3
clean:
		rm -f $(TARGET) *.o