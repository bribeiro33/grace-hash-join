# Compiler
CC = g++

CFLAGS = -g -Wall -Wextra -pedantic -std=c++14

OBJECTS = Record.o Page.o Disk.o Mem.o Bucket.o Join.o

TARGET = GHJ

# make object files
%.o: %.cpp
	$(CC) $(CFLAGS) $*.cpp -c -o $@

.PHONY: all
all: exec

# main executable
exec: main.cpp $(OBJECTS)
	$(CC) $(CFLAGS) $(OBJECTS) main.cpp -o $(TARGET)

.PHONY: clean
clean:
	rm -rf *.o $(TARGET) *.dSYM