CFLAGS=-Werror -Wall -std=c++11 -mavx2 -pedantic -g -I/usr/local/include

LDFLAGS=-L/usr/local/lib -lboost_atomic -lboost_system -lboost_thread -lglog #-ljemalloc
CC=g++

all: fineTree_test

fineTree_test: main.cpp fineTree.h
	$(CC) $(CFLAGS) -o fineTree_test main.cpp $(LDFLAGS)

clean:
	rm -rf fineTree_test *.o