#define NDEBUG

#include <iostream>
#include <string>
#include <assert.h>
#include "palmtree.h"
#include <thread>
#include <cstdlib>
#include <glog/logging.h>
#include <map>

#define TEST_SIZE 1024000
using namespace std;

void test() {
  palmtree::PalmTree<int, int> palmtree(std::numeric_limits<int>::min());
  palmtree::PalmTree<int, int> *palmtreep = &palmtree;

  for (int i = 0; i < 32; i++) {
    palmtreep->insert(i, i);
  }

  for (int i = 16; i <= 30; i++) {
     palmtreep->remove(i);
  }

  for (int i = 0; i <= 15; i++) {
    palmtreep->remove(i);
  }

  palmtreep->remove(31);

  for (int i = 0; i < 32; i++) {
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

  for (auto itr = reference.begin(); itr != reference.end(); itr++) {
    DLOG(INFO) << itr->first << " " << itr->second;
  }

  for (int i = 246; i < 256; i++) {
    int res;
    bool suc = palmtreep->find(i, res);
    CHECK(suc == true && res == reference[i]) << "Should find " << i << " " << reference[i];
  }

  while(palmtree.task_nums > 0)
    ;
}

void bench() {
  int *buff = new int[TEST_SIZE];
  for(int i = 0; i < TEST_SIZE; i++) {
    buff[i] = i;
  }

  std::random_shuffle(buff, buff + TEST_SIZE);

  palmtree::PalmTree<int, int> palmtree(std::numeric_limits<int>::min());
  palmtree::PalmTree<int, int> *palmtreep = &palmtree;

  std::vector<std::thread> threads;

  for (int i = 0; i < 1; i++) {
    threads.push_back(std::thread([palmtreep, i, buff]() {
      for(int j = 0; j < TEST_SIZE; j++) {
        auto kv = buff[j];
        // LOG(INFO) << "thread " << i << " turn " << j;
        int res;
        palmtreep->insert(kv, kv);
        bool success = palmtreep->find(kv, res);

        if (success) {
          DLOG(INFO) << "Thread " << i << " get " << res;
        } else {
          CHECK(false) << "It should find something in round " << i;
        }
      }
    }));
  }

  for (auto &thread : threads)
    thread.join();

  delete buff;
  LOG(INFO) << "task_nums: " << palmtree.task_nums;
  while(palmtree.task_nums > 0)
    ;

}

int main(int argc, char *argv[]) {
  // Google logging
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  DLOG(INFO) << "hello world";

  bench();

  return 0;
}

