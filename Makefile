CFLAGS=-Werror -Wall -std=c++11 -O2 -I/usr/local/include
LDFLAGS=-L/usr/local/lib/ -lboost_atomic-mt -lboost_system-mt -lboost_thread-mt -lglog
CC=g++

all: palmtree_test map_test

palmtree_test: main.cpp palmtree.h
	$(CC) $(CFLAGS) -o palmtree_test main.cpp $(LDFLAGS)

map_test: map_test.cpp
	$(CC) $(CFLAGS) -o map_test map_test.cpp $(LDFLAGS)

clean:
	rm -rf palmtree_test map_test *.o
