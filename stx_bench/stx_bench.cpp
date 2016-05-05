//
// Created by Zrs_y on 5/4/16.
//
#include <iostream>
#include <stx/btree_map.h>
#include <glog/logging.h>
#include <iostream>
#include <cstdlib>
#include <time.h>
#include <unistd.h>
#include "../CycleTimer.h"

class fast_random {
public:
  fast_random(unsigned long seed) : seed(0) { set_seed0(seed); }

  inline unsigned long next() {
    return ((unsigned long)next(32) << 32) + next(32);
  }

  inline uint32_t next_u32() { return next(32); }

  inline uint16_t next_u16() { return (uint16_t)next(16); }

  /** [0.0, 1.0) */
  inline double next_uniform() {
    return (((unsigned long)next(26) << 27) + next(27)) / (double)(1L << 53);
  }

  inline char next_char() { return next(8) % 256; }

  inline char next_readable_char() {
    static const char readables[] =
      "0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    return readables[next(6)];
  }

  inline std::string next_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++) s[i] = next_char();
    return s;
  }

  inline std::string next_readable_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++) s[i] = next_readable_char();
    return s;
  }

  inline unsigned long get_seed() { return seed; }

  inline void set_seed(unsigned long seed) { this->seed = seed; }

private:
  inline void set_seed0(unsigned long seed) {
    this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
  }

  inline unsigned long next(unsigned int bits) {
    seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    return (unsigned long)(seed >> (48 - bits));
  }

  unsigned long seed;
};


void readonly_bench(size_t entry_count, size_t read_count) {

  LOG(INFO) << "Running std map";
  stx::btree_map<int, int> map;
  for (size_t i = 0; i < entry_count; i++)
    map.insert(std::make_pair(i, i));

  fast_random rng(time(0));
  auto start = CycleTimer::currentSeconds();
  for (size_t i = 0; i < read_count; i++) {
    int rand_key = rng.next_u32() % entry_count;
    map.find(rand_key);
  }
  auto end = CycleTimer::currentSeconds();
  LOG(INFO) << "stx map run for " << end-start << "s, " << "thput:" << std::fixed << read_count/(end-start)/1000 << " K rps";

}

int main() {
  readonly_bench(1024*512, 1024*1024*10);
  return 0;
}
