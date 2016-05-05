# CFLAGS=-Werror -Wall -std=c++11 -g -ggdb -I/usr/local/include
CFLAGS=-Werror -Wall -std=c++11  -O3 -I/usr/local/include
LDFLAGS=-L/usr/local/lib  -lglog
CC=g++

all: stx_bench

stx_bench: stx_bench.cpp
	$(CC) $(CFLAGS) -o stx_bench stx_bench.cpp $(LDFLAGS)


clean:
	rm -rf stx_bench *.o
