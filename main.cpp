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

  palmtree::PalmTree<string, string> palmtree;
  palmtree::PalmTree<string, string> *palmtreep = &palmtree;

  std::vector<std::thread> threads;
  for (int i = 0; i < 256; i++) {
    threads.push_back(std::thread([palmtreep]() {
      string *res = palmtreep->find("hello");
      assert(res == nullptr);
    }));
  }

  for (auto &thread : threads)
    thread.join();
}

