#include <iostream>
#include "fineTree.h"
using namespace std;

int main() {
  cout << "Hello, World!" << endl;

  fineTree<int, int> fTree(0xffffffff);
  auto res = fTree.search(1);

  return res;
}