#include <iostream>
#include <string>
#include <assert.h>
#include "palmtree.h"

using namespace std;

int main(int argc, char **argv) {
  cout << "hello world" << endl;
  palmtree::PalmTree<string, string> palmtree;
  string *res = palmtree.Find("hello");
  assert(res == nullptr);
}

