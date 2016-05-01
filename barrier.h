//
// Created by Zrs_y on 4/28/16.
//

#pragma once

#include <boost/atomic.hpp>

class Barrier {
public:
  Barrier() = delete;
  Barrier(int n): P(n) {
    // printf("init\n");
    m_generation = 0;
    m_count = n;
  }

  bool wait() {
    lock.lock();
    auto gen = m_generation.load();

    if (--m_count == 0) {
      m_generation++;
      m_count = P;
      lock.unlock();
      // printf("last count is %d\n", m_count.load());
      return true;
    }

    lock.unlock();

    // printf("count is %d gen %ld, glob %ld\n", m_count.load(), gen, m_generation.load());
    while (gen == m_generation);
    // printf("leave\n");
    return false;
  }

private:
  class spinlock {
  public:
    spinlock() {
      next_ticket = 0;
      now_serving = 0;
    }

    void lock() {
      auto my_ticket = next_ticket++;
      while(my_ticket != now_serving) ;
    }

    void unlock() {
      now_serving++;
    }

  private:
    std::atomic<unsigned int> next_ticket;
    std::atomic<unsigned int> now_serving;
  };

  spinlock lock;
  std::atomic<int> m_count;
  std::atomic<unsigned long> m_generation;
  int P;
};