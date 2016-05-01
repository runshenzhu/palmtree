//
// Created by Zrs_y on 4/28/16.
//
#include <stdio.h>
#include <iostream>
#include <map>
#include "CycleTimer.h"

#define TEST_SIZE 10240000

int main() {

  int *buff = new int[TEST_SIZE];
  for(int i = 0; i < TEST_SIZE; i++) {
    buff[i] = i;
  }

  std::random_shuffle(buff, buff + TEST_SIZE);

  auto begin_time = CycleTimer::currentSeconds();
  std::map<int, int> t;

  for(int i = 0; i < TEST_SIZE; i++) {
    auto kv = buff[i];
    t[kv] = kv;
    if (t[kv] != kv) {
      return 0;
    }
  }

  auto end_time = CycleTimer::currentSeconds();
  std::cout << "dict's size is " << t.size() << std::endl;
  std::cout << "running time is " << end_time - begin_time << " seconds" << std::endl;
  delete buff;
  return 0;
}
