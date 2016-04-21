#pragma once

#include <functional>
#include <vector>
#include <unordered_map>
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
      KeyType lower_bound;
      Node *parent;

      Node(): slot_used(0), parent(nullptr){};
      Node(Node *p): slot_used(0), parent(p){};
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
      ValueType values[LEAF_MAX_SLOT+1];

      virtual NodeType type() const {
        return LEAFNODE;
      }

      inline bool is_full() const {
        return Node::slot_used == LEAF_MAX_SLOT;
      }

      inline bool is_few() const {
        return Node::slot_used < LEAF_MAX_SLOT/4;
      }

      inline ValueType *get_value(const KeyType &key UNUSED) {
        return &values[0];
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
      // Now use busy waiting, should use something more smart. But be careful
      // that conditional variable could be very expensive
      inline void wait() {
        while (!done_) {
          boost::this_thread::sleep_for(boost::chrono::milliseconds(10));
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
      NodeMod(const TreeOp &op) {
        CHECK(op.op_type_ == TREE_OP_FIND) << "NodeMod can't convert from a find operation" << endl;
        if (op.op_type_ == TREE_OP_REMOVE) {
          this->type_ = MOD_TYPE_DEC;
        } else {
          this->type_ = MOD_TYPE_ADD;
        }
      }
      ModType type_;
      // For leaf modification
      std::vector<std::pair<KeyType, ValueType>> value_items;
      // For inner node modification
      std::vector<std::pair<KeyType, Node *>> node_items;
      // For removed keys
      std::vector<std::pair<KeyType, ValueType>> orphaned_kv;
    };

  /********************
   * PalmTree private
   * ******************/
  private:
    // Root of the palm tree
    Node *tree_root;
    // Height of the tree
    int tree_depth_;
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
    // Return the index of the largest slot whose key <= @target
    // assume there is no duplicated element
    int search_helper(const KeyType *input, int size, const KeyType &target) {
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

      return res;
    }

    /**
     * @brief Return the leaf node that contains the @key
     */
    LeafNode *search(const KeyType &key UNUSED) {

      auto ptr = (InnerNode *)tree_root;
      for (;;) {
        assert(ptr->slot_used > 0);
        auto idx = this->search_helper(ptr->keys, ptr->slot_used, key);
        CHECK(idx != -1) << "search innerNode fail" << endl;
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
     * @brief big_split will split the kv pair vector into multiple tree nodes
     *  that is within the threshold. The actual type of value is templated as V.
     *  The splited nodes should be stored in Node, respect to appropriate
     *  node types
     */
    void big_split_leaf(std::vector<std::pair<KeyType, ValueType>> &input UNUSED, LeafNode *node, std::vector<std::pair<KeyType, Node *>> &new_nodes UNUSED) {

      std::sort(input.begin(), input.end(), [this](const std::pair<KeyType, ValueType> &p1, const std::pair<KeyType, ValueType> &p2) {
        return key_less(p1.first, p2.first);
      });

      int half_size = (int) input.size() / 2;
      auto itr = input.begin();

      // save first half items (small part) in old node
      node->slot_used = 0;
      for(int i = 0; i < half_size; i++) {
        add_item_leaf((LeafNode *) node, (*itr).first, (*itr).second);
        itr++;
      }


      LeafNode* new_node = new LeafNode();


      // save the second-half in new node
      auto new_key = (*itr).first;
      while(itr != input.end()){
        add_item_leaf((LeafNode *) new_node, (*itr).first, (*itr).second);
        itr++;
      }

      new_node->next = node->next;
      node->next->prev = new_node;
      node->next = new_node;
      new_node->prev = node;

      new_nodes.push_back(std::make_pair(new_key, new_node));
    }


    void big_split_inner(std::vector<std::pair<KeyType, Node *>> &input UNUSED, InnerNode *node, std::vector<std::pair<KeyType, Node *>> &new_nodes UNUSED) {

      std::sort(input.begin(), input.end(), [this](const std::pair<KeyType, Node *> &p1, const std::pair<KeyType, Node *> &p2) {
        return key_less(p1.first, p2.first);
      });

      int half_size = (int) input.size() / 2;
      auto itr = input.begin();

      // save first half items (small part) in old node
      node->slot_used = 0;
      for(int i = 0; i < half_size; i++) {
        add_item_inner(node, (*itr).first, (*itr).second);
        itr++;
      }


      InnerNode* new_node = new InnerNode();


      // save the second-half in new node
      auto new_key = (*itr).first;
      while(itr != input.end()){
        add_item_inner(new_node, (*itr).first, (*itr).second);
        itr++;
      }



      new_nodes.push_back(std::make_pair(new_key, new_node));
    }

    void add_item_inner(InnerNode *node UNUSED, const KeyType &key UNUSED, Node *child UNUSED) {
      auto idx = node->slot_used++;
      node->keys[idx] = key;
      node->children[idx] = child;
      return;
    }

    void add_item_leaf(LeafNode *node UNUSED, const KeyType &key UNUSED, const ValueType &val UNUSED) {
      auto idx = node->slot_used++;
      node->keys[idx] = key;
      node->values[idx] = val;
      return;
    }

    void del_item_inner(InnerNode *node UNUSED, const KeyType &key UNUSED) {
      auto lastIdx = node->slot_used - 1;
      auto idx = search_helper(node->keys, node->slot_used, key);
      DLOG(INFO) << "search in del, idx: " << idx;
      if (idx == -1) {
        DLOG(WARNING) << "del fail, can't find key in node";
        return;
      }

      if(!key_eq(key, node->keys[idx])) {
        DLOG(WARNING) << "del in inner, del idx: " << idx << " key != del_key" << endl;
      }

      // if it's the last element
      // just pop it
      if (idx == lastIdx) {
        node->slot_used--;
        return;
      }

      // otherwise, swap
      node->keys[idx] = node->keys[lastIdx];
      node->children[idx] = node->children[lastIdx];

      node->slot_used--;
      return;
    }

    void del_item_leaf(LeafNode *node UNUSED, const KeyType &key UNUSED) {
      auto lastIdx = node->slot_used - 1;
      auto idx = search_helper(node->keys, node->slot_used, key);
      DLOG(INFO) << "search in del, idx: " << idx;
      if (idx == -1 || !key_eq(node->keys[idx], key)) {
        DLOG(WARNING) << "del fail, can't find key in node";
        return;
      }

      // if it's the last element
      // just pop it
      if (idx == lastIdx) {
        node->slot_used--;
        return;
      }

      // otherwise, swap
      node->keys[idx] = node->keys[lastIdx];
      node->values[idx] = node->values[lastIdx];

      node->slot_used--;
      return;
    }


    // collect kv pairs in (or under) this node
    // used for merge
    void collect_leaf(Node *node, std::vector<std::pair<KeyType, ValueType>> &container) {
      if(node->type() == LEAFNODE) {
        auto ptr = (LeafNode *)node;
        for(int i = 0; i < node->slot_used; i++) {
          container.push_back(std::make_pair(ptr->keys[i], ptr->values[i]));
        }
      }else if(node->type() == INNERNODE) {
        auto ptr = (InnerNode *)node;
        for(int i = 0; i < node->slot_used; i++) {
          collect_leaf(ptr->children[i], container);
        }
      }else{
        assert(0);
      }

      return;
    }

    /**
     * @brief Modify @node by applying node modifications in @modes. If @node
     * is a leaf node, @mods will be a list of add kv and del kv. If @node is
     * a inner node, @mods will be a list of add range and del range. If new
     * node modifications are triggered, record them in @new_mods.
     */
    NodeMod modify_node(Node *node UNUSED, const std::vector<NodeMod> &mods UNUSED) {
      if(node->type() == LEAFNODE) {
        return modify_node_leaf((LeafNode *)node, mods);
      }else{
        CHECK(node->type() == INNERNODE) << "unKnown node" << endl;
        return modify_node_inner((InnerNode *)node, mods);
      }
    }

    NodeMod modify_node_leaf(LeafNode *node UNUSED, const std::vector<NodeMod> &mods UNUSED) {
      NodeMod ret(MOD_TYPE_NONE);
      auto& kv = ret.orphaned_kv;

      // randomly pick up a key, used for merge
      auto node_key = node->keys[0];

      // firstly, we loop all items to save orphaned and count nodes
      int num = node->slot_used;
      for (auto& item : mods) {
        // save all orphaned_*
        kv.insert(kv.end(), item.orphaned_kv.begin(), item.orphaned_kv.end());

        auto item_size = (int)item.value_items.size();
        if (item.type_ == MOD_TYPE_ADD) {
          num += item_size;
        } else if (item.type_ == MOD_TYPE_DEC) {
          num -= item_size;
        } else {
          assert(item_size == 0);
        }
      }

      if (num >= LEAF_MAX_SLOT) {
        auto comp = [this](const std::pair<KeyType, ValueType> &p1, const std::pair<KeyType, ValueType> &p2) {
          return key_less(p1.first, p2.first);
        };

        std::set<std::pair<KeyType, ValueType>, decltype(comp)> buf(comp);

        // execute add/del
        for (auto& item : mods) {
          if (item.type_ == MOD_TYPE_ADD) {
            for (auto& kv : item.value_items) {
              buf.insert(kv);
            }
          } else if(item.type_ == MOD_TYPE_DEC) {
            for (auto& kv : item.value_items) {
              if(buf.count(kv)) {
                buf.erase(kv);
              }else{
                del_item_leaf(node, kv.first);
              }
            }
          }
        }

        // construct input for split
        auto split_input = std::vector<std::pair<KeyType, ValueType>>(buf.size() + node->slot_used);
        for(auto itr = buf.begin(); itr != buf.end(); itr++) {
          split_input.push_back(*itr);
        }

        for(auto i = 0; i < node->slot_used; i++) {
          split_input.push_back(std::make_pair(node->keys[i], node->values[i]));
        }
        // do split based on this buf
        big_split_leaf(split_input, node, ret.node_items);
        ret.type_ = MOD_TYPE_ADD;
        return ret;
      } else {
        for (auto& item : mods) {
          if (item.type_ == MOD_TYPE_ADD) {
            for (auto& kv : item.value_items) {
              add_item_leaf(node, kv.first, kv.second);
            }
          } else if(item.type_ == MOD_TYPE_DEC) {
            for (auto& kv : item.value_items) {
              del_item_leaf(node, kv.first);
            }
          }
        }
      }

      // merge
      // fixme: never merge the first leafnode
      // because the min_key is in this node
      // we can't delete min_key
      if (node->is_few() && node->prev != nullptr) {
        collect_leaf(node, ret.orphaned_kv);
        ret.node_items.push_back(std::make_pair(node_key, node));
        ret.type_ = MOD_TYPE_DEC;
      }

      return ret;
    }


    NodeMod modify_node_inner(InnerNode *node UNUSED, const std::vector<NodeMod> &mods UNUSED) {
      NodeMod ret(MOD_TYPE_NONE);
      auto& kv = ret.orphaned_kv;

      // randomly pick up a key, used for merge
      auto node_key = node->keys[0];

      // firstly, we loop all items to save orphaned and count nodes
      int num = node->slot_used;
      for (auto& item : mods) {
        // save all orphaned_*
        kv.insert(kv.end(), item.orphaned_kv.begin(), item.orphaned_kv.end());

        auto item_size = (int)item.node_items.size();
        if (item.type_ == MOD_TYPE_ADD) {
          num += item_size;
        } else if (item.type_ == MOD_TYPE_DEC) {
          num -= item_size;
        } else {
          assert(item_size == 0);
        }
      }

      if (num >= LEAF_MAX_SLOT) {
        auto comp = [this](const std::pair<KeyType, Node *> &p1, const std::pair<KeyType, Node *> &p2) {
          return key_less(p1.first, p2.first);
        };

        std::set<std::pair<KeyType, Node *>, decltype(comp)> buf(comp);

        // execute add/del
        for (auto& item : mods) {
          if (item.type_ == MOD_TYPE_ADD) {
            for (auto& kv : item.node_items) {
              buf.insert(kv);
            }
          } else if(item.type_ == MOD_TYPE_DEC) {
            for (auto& kv : item.node_items) {
              if(buf.count(kv)) {
                buf.erase(kv);
              }else{
                del_item_inner(node, kv.first);
              }
            }
          }
        }

        // construct input for split
        auto split_input = std::vector<std::pair<KeyType, Node *>>(buf.size() + node->slot_used);
        for(auto itr = buf.begin(); itr != buf.end(); itr++) {
          split_input.push_back(*itr);
        }

        for(auto i = 0; i < node->slot_used; i++) {
          split_input.push_back(std::make_pair(node->keys[i], node->children[i]));
        }
        // do split based on this buf
        big_split_inner(split_input, node, ret.node_items);
        ret.type_ = MOD_TYPE_ADD;
        return ret;
      } else {
        for (auto& item : mods) {
          if (item.type_ == MOD_TYPE_ADD) {
            for (auto& kv : item.node_items) {
              add_item_inner(node, kv.first, kv.second);
            }
          } else if(item.type_ == MOD_TYPE_DEC) {
            for (auto& kv : item.node_items) {
              del_item_inner(node, kv.first);
            }
          }
        }
      }

      // merge
      // fixme: don't delete min_key
      if (node->is_few()) {
        collect_leaf(node, ret.orphaned_kv);
        ret.node_items.push_back(std::make_pair(node_key, node));
        ret.type_ = MOD_TYPE_DEC;
      }

      return ret;
    }

    /**************************
     * Concurrent executions **
     *
     * Design: we have a potential infinite long task queue, where clients add
     * requests by calling find, insert or remove. We also have a fixed length
     * pool of worker threads. One of the thread (thread 0) will collect task from the
     * work queue, if it has collected enough task for a batch, or has timed out
     * before collecting enough tasks, it will partition the work and start the
     * Palm algorithm among the threads.
     * ************************/
    boost::barrier barrier_;
    boost::lockfree::queue<TreeOp *> task_queue_;
    // The current batch that is being processed
    std::vector<TreeOp *> current_batch_;

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
      // Node modifications on each layer, the size of the vector will be the
      // same as the tree height
      typedef std::unordered_map<Node *, std::vector<NodeMod>> NodeModsMapType;
      std::vector<NodeModsMapType> node_mods_;
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
        palmtree_->current_batch_.clear();
        while (palmtree_->current_batch_.size() != BATCH_SIZE) {
          TreeOp *op = nullptr;
          bool res = palmtree_->task_queue_.pop(op);
          if (res) {
            palmtree_->current_batch_.push_back(op);
          } else
            boost::this_thread::sleep_for(boost::chrono::milliseconds(100));
        }

        DLOG(INFO) << "Collected a batch of " << palmtree_->current_batch_.size();

        // Partition the task among threads
        const int task_per_thread = palmtree_->current_batch_.size() / NUM_WORKER;
        for (int i = 0; i < BATCH_SIZE; i += task_per_thread) {
          // Clear old tasks
          palmtree_->workers_[i/task_per_thread].current_tasks_.clear();
          for (int j = 0; j < task_per_thread; j++)
            palmtree_->workers_[i/task_per_thread].current_tasks_
              .push_back(palmtree_->current_batch_[i+j]);
        }

        // According to the paper, a pre-sort of the batch of tasks might
        // be beneficial
      }

      // Redistribute the tasks on leaf node, filter any read only task
      void redistribute_leaf_tasks(std::unordered_map<Node *, std::vector<TreeOp *>> &result) {
        // First add current tasks
        for (auto op : current_tasks_) {
          if (op->op_type_ != TREE_OP_FIND) {
            if (result.find(op->target_node_) == result.end()) {
              result.emplace(op->target_node_, std::vector<TreeOp *>());
            }
            result[op->target_node_].push_back(op);
          }
        }

        // Then remove nodes that don't belong to the current worker
        for (int i = 0; i < worker_id_; i++) {
          WorkerThread &wthread = palmtree_->workers_[i];
          for (auto op : wthread.current_tasks_) {
            result.erase(op->target_node_);
          }
        }

        // Then add the operations that belongs to a node of mine
        for (int i = worker_id_+1; i < NUM_WORKER; i++) {
          WorkerThread &wthread = palmtree_->workers_[i];
          for (auto op : wthread.current_tasks_) {
            if (result.find(op->target_node_) != result.end()) {
              result[op->target_node_].push_back(op);
            }
          }
        }

        DLOG(INFO) << "Worker " << worker_id_ << " has " << result.size() << " after task redistribution";
      }

      /**
       * @brief redistribute inner node tasks for the current thread. It will
       * read @depth layer's information about node modifications and determine
       * tasks that belongs to the current thread.
       *
       * @param layer which layer's modifications are we trying to colelct
       * @param cur_mods the collected tasks will be stored in @cur_mods
       */
      void redistribute_inner_tasks(int layer, NodeModsMapType &cur_mods) {
        cur_mods = node_mods_[layer];

        // discard
        for (int i = 0; i < worker_id_; i++) {
          auto &wthread = palmtree_->workers_[i];
          for (auto other_itr = wthread.node_mods_[layer].begin(); other_itr != wthread.node_mods_[layer].end(); other_itr++) {
            cur_mods.erase(other_itr->first);
          }
        }

        // Steal work from other threads
        for (int i = worker_id_+1; i < NUM_WORKER; i++) {
          auto &wthread = palmtree_->workers_[i];
          for (auto other_itr = wthread.node_mods_[layer].begin(); other_itr != wthread.node_mods_[layer].end(); other_itr++) {
            auto itr = cur_mods.find(other_itr->first);
            if (itr != cur_mods.end()) {
              auto &my_mods = itr->second;
              auto &other_mods = other_itr->second;
              my_mods.insert(my_mods.end(), other_mods.begin(), other_mods.end());
            }
          }
        }
      }

      /**
       * @brief carry out all operations on the tree in a serializable order,
       *  reduce operations on the same key. The result of this function is to
       *  provide proper return result for all the operations, as well as filter
       *  out the todo node modifications on the #0 layer
       *  */
      void resolve_hazards(const std::unordered_map<Node *, std::vector<TreeOp *>> &tree_ops UNUSED) {

      }

      // Handle root modifications
      void handle_root() {

      }

      // Worker loop: process tasks
      void worker_loop() {
        while (!done_) {
          // Stage 0, collect work batch and partition
          DLOG_IF(INFO, worker_id_ == 0) << "Stage 0";
          if (worker_id_ == 0) {
            collect_batch();
          }
          palmtree_->sync();
          DLOG_IF(INFO, worker_id_ == 0) << "Stage 0 finished";
          // Stage 1, Search for leafs
          DLOG(INFO) << "Worker " << worker_id_ << " got " << current_tasks_.size() << " tasks";
          DLOG_IF(INFO, worker_id_ == 0) << "Stage 1: search for leaves";
          for (auto op : current_tasks_) {
            op->target_node_ = palmtree_->search(op->key_);
            if (op->op_type_ == TREE_OP_FIND) {
              if (op->target_node_ != nullptr) // It should not be nullptr, but now we are rapidly developing!
                op->result_ = op->target_node_->get_value(op->key_);
            }
          }
          palmtree_->sync();
          DLOG_IF(INFO, worker_id_ == 0) << "Stage 1 finished";
          // Stage 2, redistribute work, read the tree then modify, each thread
          // will handle the nodes it has searched for, except the nodes that
          // have been handled by workers whose worker_id is less than me.
          // Currently we use a unordered_map to record the ownership of tasks upon
          // certain nodes.
          std::unordered_map<Node *, std::vector<TreeOp *>> collected_tasks;
          redistribute_leaf_tasks(collected_tasks);
          resolve_hazards(collected_tasks);
          // Modify nodes
          auto &upper_mods = node_mods_[1];
          auto &cur_mods = node_mods_[0];
          upper_mods.clear();
          for (auto itr = cur_mods.begin() ; itr != cur_mods.end(); itr++) {
            auto node = itr->first;
            auto &mods = itr->second;
            CHECK(node != nullptr) << "Modifying a null node";
            auto upper_mod = palmtree_->modify_node(node, mods);
            // FIXME: now we have orphaned_keys
            if (upper_mod.type_ == MOD_TYPE_NONE && upper_mod.orphaned_kv.empty()) {
              DLOG(INFO) << "No node modification happened, don't propagate upwards";
              continue;
            }
            DLOG(INFO) << "Add node modification " << upper_mod.type_ << " to upper layer";
            if (upper_mods.find(node->parent) == upper_mods.end()) {
              upper_mods.emplace(node->parent, std::vector<NodeMod>());
            }
            upper_mods[node->parent].push_back(upper_mod);
          }
          palmtree_->sync();
          DLOG_IF(INFO, worker_id_ == 0) << "Stage 2 finished";
          // Stage 3, propagate tree modifications back
          // Propagate modifications until root
          for (int layer = 1; layer <= palmtree_->tree_depth_-1; layer++) {
            NodeModsMapType cur_mods;
            redistribute_inner_tasks(layer, cur_mods);
            auto &upper_mods = node_mods_[layer+1];
            upper_mods.clear();
            for (auto itr = cur_mods.begin(); itr != cur_mods.end(); itr++) {
              auto node = itr->first;
              auto &mods = itr->second;
              auto mod_res = palmtree_->modify_node(node, mods);
              if (upper_mods.count(node->parent) == 0) {
                upper_mods.emplace(node->parent, std::vector<NodeMod>());
              }
              upper_mods[node->parent].push_back(mod_res);
            }
            palmtree_->sync();
            DLOG_IF(INFO, worker_id_ == 0) << "Layer #" << layer << " done";
          } // End propagate
          DLOG_IF(INFO, worker_id_ == 0) << "Stage 3 finished";
          // Stage 4, modify the root, re-insert orphaned, mark work as done
          if (worker_id_ == 0) {
            // Mark tasks as done
            handle_root();
            for (auto &wthread : palmtree_->workers_) {
             for (auto op : wthread.current_tasks_) {
               op->done_ = true;
             }
            }
          }
        } // End worker loop
      }
    }; // End WorkerThread


    std::vector<WorkerThread> workers_;
    /**********************
     * PalmTree public    *
     * ********************/
  public:
    PalmTree(): tree_depth_(0), barrier_{NUM_WORKER}, task_queue_{10240} {
      init();
    }

    void init() {
      // Init the root node
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
      for (auto &worker : workers_) {
        worker.exit();
      }
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

