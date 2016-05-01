CFLAGS=-Werror -Wall -std=c++11 -O2 -I/usr/local/Cellar/boost/1.60.0_2/include -I/usr/local/Cellar/glog/0.3.4/include -I/usr/local/Cellar/gflags/2.1.2/include
LDFLAGS=-L/usr/local/Cellar/boost/1.60.0_2/lib -L/usr/local/Cellar/glog/0.3.4/lib -lboost_atomic-mt -lboost_system-mt -lboost_thread-mt -lglog
CC=g++

all: palmtree_test map_test

palmtree_test: main.cpp palmtree.h
	$(CC) $(CFLAGS) -o palmtree_test main.cpp $(LDFLAGS)

map_test: map_test.cpp
	$(CC) $(CFLAGS) -o map_test map_test.cpp $(LDFLAGS)

clean:
	rm -rf palmtree_test map_test *.o
