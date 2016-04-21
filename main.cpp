#include <iostream>
#include <string>
#include <assert.h>
#include "palmtree.h"
#include <thread>
#include <glog/logging.h>

using namespace std;

int main(int argc, char *argv[]) {
  // Google logging
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  DLOG(INFO) << "hello world";

  palmtree::PalmTree<int, int> palmtree(std::numeric_limits<int>::min());
  palmtree::PalmTree<int, int> *palmtreep = &palmtree;

  std::vector<std::thread> threads;
  for (int i = 0; i < 256; i++) {
    threads.push_back(std::thread([palmtreep, i]() {
      int res;
      palmtreep->insert(i, i);
      bool success = palmtreep->find(i, res);
      if (success) {
        DLOG(INFO) << "Thread " << i << " get " << res;
      } else {
        assert(false);
      }
    }));
  }

  for (auto &thread : threads)
    thread.join();
}

