//
// Created by Runshen Zhu on 4/28/16.
//

#pragma once
#include <atomic>

// a re-useable barrier for sync-ing among multiple working threads
class Barrier {
public:
  Barrier() = delete;
  Barrier(int n): P(n) {
    m_generation = 0;
    m_count = n;
  }

  // wait blocks until all P threads arrive the barrier and call it.
  bool wait() {
    lock.lock();
    auto gen = m_generation.load();

    if (--m_count == 0) {
      m_generation++;
      m_count = P;
      lock.unlock();
      return true;
    }

    lock.unlock();

    while (gen == m_generation);
    return false;
  }

private:
  class spinlock {
  // a tick based spinlock
  // traditional CAS spin lock has lots of bus traffic
  // this implementation is aimed to ease the bus traffic
  // 
  // traditional CAS lock:
  //    let const int unlock = 1, lock = 0;
  //    lock() { while( CAS(LOCK_, unlock, lock) == false ) {} }
  // 
  // each CAS call is equal to a bus write, which invalids cache line
  // and bring bus traffic. So it's not a good idea to keep CAS-ing on
  // the value LOCK_
  //
  // a simple way to ease it is to use `test and CAS` approach:
  //    lock() {
  //        for(;;) {
  //            if (LOCK_ == unlock && CAS(LOCK_, unlock, lock) == true) {
  //                return;
  //            } 
  //        }
  //    }
  //
  // compared with `test and CAS` approach, tick based spin lock takes a further
  // step and reduces more traffic than `test and CAS` spin lock. (because there are
  // still confilicts in `test and CAS` approach)
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

  // # of threads that haven't arrived this barrier
  std::atomic<int> m_count;
  // generation of this barrier
  std::atomic<unsigned long> m_generation;
  // # of threads that are using this barrier
  int P;
};