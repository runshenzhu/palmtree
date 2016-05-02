# CFLAGS=-Werror -Wall -std=c++11 -g -ggdb
CFLAGS=-Werror -Wall -std=c++11 -O3
LDFLAGS=-L/usr/local/lib -lboost_atomic -lboost_system -lboost_thread -lglog
CC=g++

all: palmtree_test

palmtree_test: main.cpp palmtree.h
	$(CC) $(CFLAGS) -o palmtree_test main.cpp $(LDFLAGS)

map_test: map_test.cpp
	$(CC) -std=c++11 -O3 -o map_test map_test.cpp
	time ./map_test

clean:
	rm -rf palmtree_test map_test *.o
