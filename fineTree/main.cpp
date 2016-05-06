#include <iostream>
#include <glog/logging.h>
#include "fineTree.h"
using namespace std;

int main() {
  cout << "Hello, World!" << endl;

  fineTree<int, int> fTree(0xffffffff);

  auto entry_count = 1024 * 512;
  int *buff = new int[entry_count];
  for(int i = 0; i < entry_count; i++) {
    buff[i] = i;
  }

  std::random_shuffle(buff, buff + entry_count);

  for(int j = 0; j < entry_count; j++) {
    auto kv = buff[j];
    fTree.insert(kv, kv);
  }

  for(int j = 0; j < entry_count; j++) {
    int value;
    auto res = fTree.search(j, value);
    CHECK(res == 0) << "search fail";
    CHECK(value == j) << "search fail";
  }

  LOG(INFO) << "test end";

  delete buff;
}