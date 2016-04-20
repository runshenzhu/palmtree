#pragma once

#include <functional>
#include <vector>
#include <chrono>
#include <assert.h>
#include <thread>
#include <boost/lockfree/queue.hpp>
#include <boost/thread/barrier.hpp>
#include <boost/thread.hpp>
#include <iostream>
#include <glog/logging.h>

using std::cout;
using std::endl;

#define UNUSED __attribute__((unused))

namespace palmtree {

  /**
   * Tree operation types
   */
  enum TreeOpType {
    TREE_OP_FIND = 0,
    TREE_OP_INSERT,
    TREE_OP_REMOVE
  };

  enum NodeType {
    INNERNODE = 0,
    LEAFNODE
  };

  template <typename KeyType,
           typename ValueType,
           typename PairType = std::pair<KeyType, ValueType>,
           typename KeyComparator = std::less<KeyType> >
  class PalmTree {
    // Max number of slots per inner node
    static const int INNER_MAX_SLOT = 256;
    // Max number of slots per leaf node
    static const int LEAF_MAX_SLOT = 1024;
    // Threshold to control bsearch or linear search
    static const int BIN_SEARCH_THRESHOLD = 32;
    // Number of working threads
    static const int NUM_WORKER = 8;
    static const int BATCH_SIZE = 256;

  private:
    /**
     * Tree node base class
     */
    struct Node {
      // Number of actually used slots
      int slot_used;

      Node(){};
      virtual ~Node() {};
      virtual NodeType type() const = 0;
    };

    struct InnerNode : public Node {
      InnerNode(){};
      virtual ~InnerNode() {};
      // Keys for children
      KeyType keys[INNER_MAX_SLOT];
      // Pointers for children
      Node *children[INNER_MAX_SLOT];

      virtual NodeType type() const {
        return INNERNODE;
      }
      inline bool is_full() const {
        return Node::slot_used == INNER_MAX_SLOT;
      }

      inline bool is_few() const {
        return Node::slot_used < INNER_MAX_SLOT/2;
      }

    };

    struct LeafNode : public Node {
      LeafNode(): prev(nullptr), next(nullptr) {};
      virtual ~LeafNode() {};


      // Leaf layer doubly linked list
      LeafNode *prev;
      LeafNode *next;

      // Keys and values for leaf node
      KeyType keys[LEAF_MAX_SLOT];
      ValueType values[LEAF_MAX_SLOT];

      virtual NodeType type() const {
        return LEAFNODE;
      }

      inline bool is_full() const {
        return Node::slot_used == LEAF_MAX_SLOT;
      }

      inline bool is_few() const {
        return Node::slot_used < LEAF_MAX_SLOT/4;
      }
    };
    /**
     * Tree operation wrappers
     */
    struct TreeOp {
      // Op can either be none, add or delete
      TreeOp(TreeOpType op_type, const KeyType &key, const ValueType &value):
        op_type_(op_type), key_(key), value_(value), target_node_(nullptr),
        result_(nullptr), boolean_result_(false), done_(false) {};


      TreeOp(TreeOpType op_type, const KeyType &key):
        op_type_(op_type), key_(key), target_node_(nullptr), result_(nullptr),
        boolean_result_(false), done_(false) {};

      TreeOpType op_type_;
      KeyType key_;
      ValueType value_;

      LeafNode *target_node_;
      ValueType *result_;
      bool boolean_result_;
      bool done_;

      // Wait until this operation is done
      inline void wait() {
        while (!done_) {
          auto sleep_time = boost::chrono::microseconds(30);
          boost::this_thread::sleep_for(sleep_time);
        }
      }
    };

    enum ModType {
      MOD_TYPE_ADD,
      MOD_TYPE_DEC,
      MOD_TYPE_NONE
    };

    /**
     * Wrapper for node modification
     */
    struct NodeMod {
      NodeMod(ModType type): type_(type) {}
      ModType type_;
      // For leaf modification
      std::vector<std::pair<KeyType, ValueType>> value_items;
      // For inner node modification
      std::vector<std::pair<KeyType, Node *>> node_items;
      // For removed inner nodes
      std::vector<KeyType> orphaned_keys;
    };

  /********************
   * PalmTree private
   * ******************/
  private:
    // Root of the palm tree
    Node *tree_root;
    // Key comparator
    KeyComparator kcmp;
    // Return true if k1 < k2
    inline bool key_less(const KeyType &k1, const KeyType &k2) {
      return kcmp(k1, k2);
    }
    // Return true if k1 == k2
    inline bool key_eq(const KeyType &k1, const KeyType &k2) {
      return !kcmp(k1, k2) && !kcmp(k2, k1);
    }
    // Return the index of the largest slot whose key <= @key
    // assume there is no duplicated element
    int search_helper(const KeyType *input, int size, const KeyType &target) {
      assert(size > 0);
      int res = -1;
      // loop all element
      for (int i = 0; i < size; i++) {
        if(key_less(target, input[i])){
          // target < input
          // ignore
          continue;

        }
        if (res == -1 || key_less(input[res], input[i])) {
          res = i;
        }

      }

      if(res != -1) {
        return res;
      }

      // if all elements are greater than target
      // return the index slot whose key is smallest
      // re-scan
      res = 0;
      for (int i = 0; i < size; i++) {
        if(key_less(input[i], input[res])) {
          res = i;
        }
      }

      return res;
    }



    /**
     * @brief Return the leaf node that contains the @key
     */
    LeafNode *search(const KeyType &key UNUSED) {
      assert(tree_root);

      if (tree_root->type() == LEAFNODE) {
        return tree_root;
      }

      auto ptr = (InnerNode *)tree_root;
      for (;;) {
        assert(ptr->slot_used > 0);
        auto idx = this->search_helper(ptr->keys, ptr->slot_used, key);
        Node *child = ptr->children[idx];
        if (child->type() == LEAFNODE) {
          return (LeafNode *)child;
        } else {
          ptr = (InnerNode *)child;
        }
      }
      // we shouldn't reach here
      assert(0);
    }

    /**
     * @brief Modify @node by applying node modifications in @modes. If @node
     * is a leaf node, @mods will be a list of add kv and del kv. If @node is
     * a inner node, @mods will be a list of add range and del range. If new
     * node modifications are triggered, record them in @new_mods.
     */
    NodeMod modify_node(Node *node UNUSED, const std::vector<NodeMod> &mods UNUSED) {

      std::vector<KeyType> K;
      for (auto& item : mods) {
        K.insert(K.end(), item.orphaned_keys.begin(), item.orphaned_keys.end());
        if (item.type_ == MOD_TYPE_ADD) {

        }
      }
      return NodeMod(MOD_TYPE_NONE);
    }

    /**************************
     * Concurrent executions **
     *
     * Design: we have a potential infinite long task queue, where clients add
     * requests by calling find, insert or remove. We also have a fixed length
     * pool of worker threads. One of the thread (thread 0) will collect task form the
     * work queue, if it has collected enough task for a batch, or has timed out
     * before collecting enough tasks, it will partition the work and start the
     * Palm algorithm among the threads.
     * ************************/
    boost::barrier barrier_;
    boost::lockfree::queue<TreeOp *> task_queue_;

    void sync() {
      barrier_.wait();
    }

    struct WorkerThread {
      WorkerThread(int worker_id, PalmTree *palmtree):
        worker_id_(worker_id),
        palmtree_(palmtree),
        done_(false){}
      // Worker id, the thread with worker id 0 will need to be the coordinator
      int worker_id_;
      // The work for the worker at each stage
      std::vector<TreeOp *> current_tasks_;
      // Spawn a thread and run the worker loop
      boost::thread wthread_;
      // The palm tree the worker belong to
      PalmTree *palmtree_;
      bool done_;
      void start() {
        wthread_ = boost::thread(&WorkerThread::worker_loop, this);
      }
      void exit() {
        done_ = true;
        wthread_.join();
      }

      // The #0 thread is responsible to collect tasks to a batch
      void collect_batch() {
        DLOG(INFO) << "Thread " << worker_id_ << " collect tasks";
        std::vector<TreeOp *>tasks;
        while (tasks.size() != BATCH_SIZE) {
          TreeOp *op = nullptr;
          bool res = palmtree_->task_queue_.pop(op);
          if (res) {
            tasks.push_back(op);
            DLOG(INFO) << "Add one task" << endl;
          } else
            boost::this_thread::yield();
        }

        DLOG(INFO) << "Collected a batch of " << tasks.size();

        // Partition the task among threads
        const int task_per_thread = tasks.size() / NUM_WORKER;
        for (int i = 0; i < BATCH_SIZE; i += task_per_thread) {
          for (int j = 0; j < task_per_thread; j++)
            palmtree_->workers_[i/task_per_thread].current_tasks_.push_back(tasks[i+j]);
        }

        // According to the paper, a pre-sort of the batch of tasks might
        // be benificial
      }

      // Worker loop: process tasks
      void worker_loop() {
        while (!done_) {
          if (worker_id_ == 0) {
            collect_batch();
            palmtree_->sync();
          } else {
            DLOG(INFO) << "Worker " << worker_id_ << " wait barrier";
            palmtree_->sync();
          }
          // Stage 0, collect work batch and partition
          DLOG(INFO) << "Worker " << worker_id_ << " got " << current_tasks_.size() << " tasks";
          // Stage 1, Search for leafs
          // Stage 2, redistribute work, read the tree then modify
          // Stage 3, propagate tree modifications back
          // Stage 4, modify the root, re-insert orphands, mark work as done
        }
      }
    };

    std::vector<WorkerThread> workers_;
    /**********************
     * PalmTree public    *
     * ********************/
  public:
    PalmTree(): barrier_{NUM_WORKER}, task_queue_{10240} {
      // Init the root node
      init();
    }

    void init() {
      tree_root = new InnerNode();
      // Init the worker thread
      for (int worker_id = 0; worker_id < NUM_WORKER; worker_id++) {
        workers_.emplace_back(worker_id, this);
      }

      for (auto &worker : workers_) {
         worker.start();
      }
    }

    // Recursively free the resources of one tree node
    void free_recursive(Node *node UNUSED) {
      if (node->type() == INNERNODE) {
        auto ptr = (InnerNode *)node;
        for(int i = 0; i < ptr->slot_used; i++) {
          free_recursive(ptr->children[i]);
        }
      }
      delete node;
    }

    ~PalmTree() {
      free_recursive(tree_root);
    }

    /**
     * @brief Find the value for a key
     * @param key the key to be retrieved
     * @return nullptr if no such k,v pair
     */
    ValueType *find(const KeyType &key UNUSED) {
      TreeOp op(TREE_OP_FIND, key);
      task_queue_.push(&op);

      op.wait();

      return op.result_;
    }

    /**
     * @brief insert a k,v into the tree
     */
    void insert(const KeyType &key UNUSED, const ValueType &value UNUSED) {
      TreeOp op(TREE_OP_INSERT, key, value);
      task_queue_.push(&op);

      op.wait();
    }

    /**
     * @brief remove a k,v from the tree
     */
    void remove(const KeyType &key UNUSED) {
      TreeOp op(TREE_OP_REMOVE, key);
      task_queue_.push(&op);

      op.wait();
    }
  }; // End of PalmTree
  // Explicit template initialization
  template class PalmTree<int, int>;
} // End of namespace palmtree

