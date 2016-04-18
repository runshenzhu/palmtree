CFLAGS=-Werror -Wall -std=c++11
CC=g++

all: palmtree_test

palmtree_test: main.cpp palmtree.h
	$(CC) $(CFLAGS) -o palmtree_test main.cpp

clean:
	rm -rf palmtree_test *.o