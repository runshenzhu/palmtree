#include <iostream>
#include <glog/logging.h>
#include <vector>
#include <thread>
#include "fineTree.h"
#include "../CycleTimer.h"
using namespace std;

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


int main() {
  cout << "Hello, World!" << endl;

  fineTree<int, int> fTree(0xffffffff);

  auto entry_count = 1024 * 51200;
  auto read_count = 1024*1024*1;
  int *buff = new int[entry_count];
  for(int i = 0; i < entry_count; i++) {
    buff[i] = i;
  }

  std::random_shuffle(buff, buff + entry_count);

  for(int j = 0; j < entry_count; j++) {
    auto kv = buff[j];
    fTree.insert(kv, kv);
  }
  auto fp = &fTree;


  auto start = CycleTimer::currentSeconds();
  std::vector<std::thread> threads;
  int w_n = 4;
  for(int j = 0; j < w_n; j ++) {
    threads.push_back(std::thread([fp, w_n, read_count, entry_count]() {
      fast_random rng(time(0));
      for (int i = 0; i < read_count / w_n; i++) {
        int rand_key = rng.next_u32() % entry_count;
        int val;
        auto res = fp->search(rand_key, val);
        if (res != 0 || val != rand_key) {
          LOG(FATAL) << "search fail";
        }
      }
    }));
  }

  for (auto &t : threads) {
    t.join();
  }
  auto end = CycleTimer::currentSeconds();
  LOG(INFO) << "fineTree run for " << end-start << "s, " << "thput:" << std::fixed << read_count/(end-start)/1000 << " K rps";

  delete buff;
}