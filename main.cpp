#include <iostream>
#include <string>
#include <assert.h>
#include "palmtree.h"

using namespace std;

int main(int argc, char *argv[]) {
  string info_log = "~/master_info_";
  cout << "hello" << endl;
  palmtree::PalmTree<string, string> palmtree;
  palmtree.init();
  string *res = palmtree.find("hello");
  assert(res == nullptr);
}

