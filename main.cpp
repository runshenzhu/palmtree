#include <iostream>
#include <string>
#include <assert.h>
#include "palmtree.h"
#include <thread>
#include <cstdlib>
#include <glog/logging.h>
#include <map>

using namespace std;

int main(int argc, char *argv[]) {
  // Google logging
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  DLOG(INFO) << "hello world";

  palmtree::PalmTree<int, int> palmtree(std::numeric_limits<int>::min());
  palmtree::PalmTree<int, int> *palmtreep = &palmtree;

  std::vector<std::thread> threads;
  for (int i = 0; i < 16; i++) {
   // threads.push_back(std::thread([palmtreep, i]() {
      int res;
      palmtreep->insert(i, i);
      bool success = palmtreep->find(i, res);
      if (success) {
        DLOG(INFO) << "Thread " << i << " get " << res;
      } else {
        assert(false);
      }
//    }));
  }

  for (int i = 0; i < 16; i++) {
    DLOG(INFO) << "Remove " << i;
    palmtreep->remove(i);
    int res;
    DLOG(INFO) << "Find " << i;
    bool success = palmtreep->find(i, res);
    if (success) {
      assert(false);
    } else {
      DLOG(INFO) << "Thread " << i << " get nothing";
    }
  }

  srand(15618);

  std::map<int, int> reference;
  for (int i = 10; i < 256; i++) {
    int key1 = i;
    int value1 = rand() % 10;
    int key2 = i - 10;

    palmtreep->insert(key1, value1);
    palmtreep->remove(key2);

    reference.emplace(key1, value1);
    reference.erase(key2);
  }

  for (int i = 0; i < 256; i++) {
    int res;
    bool suc = palmtreep->find(i, res);
    if (reference.find(i) != reference.end()) {
      CHECK(suc == false) << "Should not find anything";
    } else {
      CHECK(suc == true && res == reference[i]) << "Should find " << i << " " << reference[i];
    }
  }

  for (auto &thread : threads)
    thread.join();
}

