CFLAGS=-Werror -Wall -std=c++11 -O3
LDFLAGS=-lboost_atomic-mt -lboost_system-mt -lboost_thread-mt -lglog
CC=g++

all: palmtree_test

palmtree_test: main.cpp palmtree.h
	$(CC) $(CFLAGS) -o palmtree_test main.cpp $(LDFLAGS)

map_test: map_test.cpp
	$(CC) -std=c++11 -O3 -o map_test map_test.cpp
	time ./map_test

clean:
	rm -rf palmtree_test *.o
