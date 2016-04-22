#pragma once

#include <functional>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <chrono>
#include <assert.h>
#include <thread>
#include <boost/lockfree/queue.hpp>
#include <boost/thread/barrier.hpp>
#include <boost/thread.hpp>
#include <iostream>
#include <memory>
#include <atomic>
#include <glog/logging.h>

using std::cout;
using std::endl;

#define UNUSED __attribute__((unused))

namespace palmtree {
  static std::atomic<int> NODE_NUM(0);
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

    static const int MAX_SLOT = 3;
    // Threshold to control bsearch or linear search
    static const int BIN_SEARCH_THRESHOLD = 32;
    // Number of working threads
    static const int NUM_WORKER = 1;
    static const int BATCH_SIZE = 1;

  private:
    /**
     * Tree node base class
     */
    struct InnerNode;
    struct Node {
      // Number of actually used slots
      int slot_used;
      int id;
      KeyType lower_bound;
      Node *parent;


      Node() = delete;
      Node(Node *p): slot_used(0), parent(p) { id = NODE_NUM++; };
      virtual ~Node() {};
      virtual std::string to_string() = 0;
      virtual NodeType type() const = 0;

    };

    struct InnerNode : public Node {
      InnerNode() = delete;
      InnerNode(Node *parent): Node(parent){};
      virtual ~InnerNode() {};
      // Keys for values
      KeyType keys[MAX_SLOT];
      // Pointers for child nodes
      Node *values[MAX_SLOT];

      virtual NodeType type() const {
        return INNERNODE;
      }

      virtual std::string to_string() {
        std::string res;
        res += "InnerNode[" + std::to_string(Node::id) + "] ";
        // res += std::to_string(Node::slot_used);
        for (int i = 0 ; i < Node::slot_used ; i++) {
          res += " " + std::to_string(keys[i]) + ":" + std::to_string(values[i]->id);
        }
        return res;
      }

      inline bool is_full() const {
        return Node::slot_used == MAX_SLOT;
      }

      inline bool is_few() const {
        return Node::slot_used < MAX_SLOT/2 || Node::slot_used == 0;
      }

    };

    struct LeafNode : public Node {
      LeafNode() = delete;
      LeafNode(Node *parent): Node(parent){};
      virtual ~LeafNode() {};

      // Keys and values for leaf node
      KeyType keys[MAX_SLOT];
      ValueType values[MAX_SLOT];
      //LeafNode *prev;
      //LeafNode *next;

      virtual NodeType type() const {
        return LEAFNODE;
      }

      virtual std::string to_string() {
        std::string res;
        res += "LeafNode[" + std::to_string(Node::id) + "] ";

//        if (Node::prev != nullptr)
//          res += "left: " + std::to_string(Node::prev->id);
//        else
//          res += "left: null";
//        res += " ";
//        if (Node::next != nullptr)
//          res += "right: " + std::to_string(Node::next->id);
//        else
//          res += "right: null";
//        res += " ";

        // res += std::to_string(Node::slot_used);
        for (int i = 0 ; i < Node::slot_used ; i++) {
          res += " " + std::to_string(keys[i]) + ":" + std::to_string(values[i]);
        }
        return res;
      }

      inline bool is_full() const {
        return Node::slot_used == MAX_SLOT;
      }

      inline bool is_few() const {
        return Node::slot_used < MAX_SLOT/4 || Node::slot_used == 0;
      }
    };
    /**
     * Tree operation wrappers
     */
    struct TreeOp {
      // Op can either be none, add or delete
      TreeOp(TreeOpType op_type, const KeyType &key, const ValueType &value):
        op_type_(op_type), key_(key), value_(value), target_node_(nullptr),
        boolean_result_(false), done_(false) {};


      TreeOp(TreeOpType op_type, const KeyType &key):
        op_type_(op_type), key_(key), target_node_(nullptr),
        boolean_result_(false), done_(false) {};

      TreeOpType op_type_;
      KeyType key_;
      ValueType value_;

      LeafNode *target_node_;
      ValueType result_;
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
        CHECK(op.op_type_ != TREE_OP_FIND) << "NodeMod can't convert from a find operation" << endl;
        if (op.op_type_ == TREE_OP_REMOVE) {
          this->type_ = MOD_TYPE_DEC;
          this->value_items.emplace_back(std::make_pair(op.key_, ValueType()));
        } else {
          this->type_ = MOD_TYPE_ADD;
          this->value_items.emplace_back(std::make_pair(op.key_, op.value_));
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
    // Minimal key
    KeyType min_key_;
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
        CHECK(ptr->slot_used > 0) << "Search empty inner node";
        auto idx = this->search_helper(ptr->keys, ptr->slot_used, key);
        CHECK(idx != -1) << "search innerNode fail" << endl;
        Node *child = ptr->values[idx];
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
    template<typename NodeType, typename V>
    void big_split(std::vector<std::pair<KeyType, V>> &input, NodeType *node, std::vector<std::pair<KeyType, Node *>> &new_nodes) {

      std::sort(input.begin(), input.end(), [this](const std::pair<KeyType, V> &p1, const std::pair<KeyType, V> &p2) {
        return key_less(p1.first, p2.first);
      });

      int half_size = (int) input.size() / 2;
      auto itr = input.begin();

      // save first half items (small part) in old node
      node->slot_used = 0;
      for (int i = 0; i < half_size; i++) {
        add_item<NodeType, V>(node, itr->first, itr->second);
        itr++;
      }

      NodeType* new_node = new NodeType(node->parent);

      // save the second-half in new node
      auto new_key = (*itr).first;
      while(itr != input.end()){
        add_item<NodeType, V>(new_node, itr->first, itr->second);
        itr++;
      }


      new_nodes.push_back(std::make_pair(new_key, new_node));
    }

    template <typename NodeType, typename V>
    void add_item(NodeType *node, const KeyType &key, V value) {
      auto idx = node->slot_used++;
      node->keys[idx] = key;
      node->values[idx] = value;
      return;
    }


    template <typename NodeType>
    void del_item(NodeType *node, const KeyType &key) {
      auto lastIdx = node->slot_used - 1;
      auto idx = search_helper(node->keys, node->slot_used, key);
      DLOG(INFO) << "search in del, idx: " << idx;
      if (idx == -1) {
        DLOG(WARNING) << "del fail, can't find key in node";
        return;
      }

      if (!key_eq(key, node->keys[idx])) {
        DLOG(WARNING) << "del in inner, del idx: " << idx << " key != del_key" << endl;
        if (node->type() == LEAFNODE)
          return;
      }

      if (node->type() == INNERNODE) {
        Node *child_node = *(Node **)(&node->values[idx]);
        DLOG(INFO) << "Delete node " << child_node->id;
        free_recursive(child_node);
      }

      auto del_key = node->keys[idx];
      if (idx == lastIdx) {
        // if it's the last element, just pop it
        node->slot_used--;
      } else {
        // otherwise, swap
        node->keys[idx] = node->keys[lastIdx];
        node->values[idx] = node->values[lastIdx];
        node->slot_used--;
      }

      // If the idx of key to be deleted is 0,
      // this key is the smallest one in the node,
      // we need to replace the second smallest key with this key
      // and place it the idx 0
      if(idx == 0 && node->type() == INNERNODE) {
        ensure_min_range((InnerNode *)node, del_key);
      }
      return;
    }

    // collect kv pairs in (or under) this node
    // used for merge
    void collect_leaf(Node *node, std::vector<std::pair<KeyType, ValueType>> &container) {
      if (node->type() == LEAFNODE) {
        auto ptr = (LeafNode *)node;
        for(int i = 0; i < node->slot_used; i++) {
          container.push_back(std::make_pair(ptr->keys[i], ptr->values[i]));
        }
      } else if (node->type() == INNERNODE) {
        auto ptr = (InnerNode *)node;
        for(int i = 0; i < node->slot_used; i++) {
          collect_leaf(ptr->values[i], container);
        }
      } else {
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
    NodeMod modify_node(Node *node, const std::vector<NodeMod> &mods) {
      DLOG(INFO) << "Modifying node " << node->id << " with " << mods.size() << " operations";
      if(node->type() == LEAFNODE) {
        return modify_node_leaf((LeafNode *)node, mods);
      }else{
        CHECK(node->type() == INNERNODE) << "unKnown node" << endl;
        return modify_node_inner((InnerNode *)node, mods);
      }
    }

    NodeMod modify_node_leaf(LeafNode *node, const std::vector<NodeMod> &mods) {
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

      DLOG(INFO) << "Result node size " << num;
      if (num >= MAX_SLOT) {
        DLOG(INFO) << "Going to split";
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
                del_item<LeafNode>(node, kv.first);
              }
            }
          }
        }

        // construct input for split
        std::vector<std::pair<KeyType, ValueType>> split_input;
        for(auto itr = buf.begin(); itr != buf.end(); itr++) {
          split_input.push_back(*itr);
        }

        for(auto i = 0; i < node->slot_used; i++) {
          split_input.push_back(std::make_pair(node->keys[i], node->values[i]));
        }
        // do split based on this buf
        big_split<LeafNode, ValueType>(split_input, node, ret.node_items);
        ret.type_ = MOD_TYPE_ADD;
        return ret;
      } else {
        DLOG(INFO) << "don't split";
        for (auto& item : mods) {
          if (item.type_ == MOD_TYPE_ADD) {
            for (auto& kv : item.value_items) {
              add_item<LeafNode, ValueType>(node, kv.first, kv.second);
            }
          } else if(item.type_ == MOD_TYPE_DEC) {
            for (auto& kv : item.value_items) {
              del_item<LeafNode>(node, kv.first);
            }
          }
        }
      }

      // merge
      // fixme: never merge the first leafnode
      // because the min_key is in this node
      // we can't delete min_key
      DLOG(INFO) << node->is_few();
      if (node->is_few()) {
        DLOG(INFO) << "Merge leaf node " << node->id;
        collect_leaf(node, ret.orphaned_kv);
        ret.node_items.push_back(std::make_pair(node_key, node));
        ret.type_ = MOD_TYPE_DEC;

//        if (node->prev != nullptr) {
//          node->prev->next = node->next;
//        }
//        if (node->next != nullptr) {
//          node->next->prev = node->prev;
//        }
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

      if (num >= MAX_SLOT) {
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
                // TODO: memleak
              }else{
                del_item<InnerNode>(node, kv.first);
              }
            }
          }
        }

        // construct input for split
        std::vector<std::pair<KeyType, Node *>> split_input;
        for(auto itr = buf.begin(); itr != buf.end(); itr++) {
          split_input.push_back(*itr);
        }

        for(auto i = 0; i < node->slot_used; i++) {
          split_input.push_back(std::make_pair(node->keys[i], node->values[i]));
        }
        // do split based on this buf
        big_split<InnerNode, Node *>(split_input, node, ret.node_items);
        for (auto itr = ret.node_items.begin(); itr != ret.node_items.end(); itr++) {
          // Reset parent, the children of the newly splited node should point
          // to the new parent
          auto new_node = itr->second;
          for (int i = 0; i < new_node->slot_used; i++) {
            CHECK(new_node->type() == INNERNODE) << " split leaf node in modify_node_inner";
            ((InnerNode *)new_node)->values[i]->parent = new_node;
          }
        }
        ret.type_ = MOD_TYPE_ADD;
        return ret;
      } else {
        for (auto& item : mods) {
          if (item.type_ == MOD_TYPE_ADD) {
            for (auto& kv : item.node_items) {
              DLOG(INFO) << "Add item " << kv.first;
              add_item<InnerNode, Node *>(node, kv.first, kv.second);
            }
          } else if(item.type_ == MOD_TYPE_DEC) {
            for (auto& kv : item.node_items) {
              DLOG(INFO) << "Del item " << kv.first;
              del_item<InnerNode>(node, kv.first);
            }
          }
        }
      }

      // merge
      if (node->is_few()) {
        collect_leaf(node, ret.orphaned_kv);
        ret.node_items.push_back(std::make_pair(node_key, node));
        ret.type_ = MOD_TYPE_DEC;

      }

      return ret;
    }

    // set the smallest key in node to min_key
    void ensure_min_range(InnerNode *node UNUSED, const KeyType &min) {
      if (node->slot_used <= 1) {
        return;
      }
      // find the second smallest
      int idx = 0;
      for(int i = 1; i < node->slot_used; i++) {
        if(key_less(node->keys[i], node->keys[idx])) {
          idx = i;
        }
      }

      CHECK(key_less(min, node->keys[idx]));

      if(idx == 0) {
        return;
      }

      // swap idx with slot 0

      /*
      auto tmp_key = node->keys[0];
      auto tmp_val = node->values[0];
      node->keys[0] = node->keys[idx];
      node->values[0] = node->values[idx];
      node->keys[idx] = tmp_key;
      node->values[idx] = tmp_val;
       */
      std::swap(node->keys[0], node->keys[idx]);
      std::swap(node->values[0], node->values[idx]);

    }

    void ensure_min_key() {
      auto ptr = (Node *)tree_root;
      while(ptr->type() == INNERNODE) {
        auto inner = (LeafNode *)ptr;
        inner->keys[0] = min_key_;
        ptr = inner->values[0];
      }
    }

    void ensure_tree_structure(Node *node, int indent = 0) {
      std::string space;
      for (int i = 0; i < indent; i++)
        space += " ";
      DLOG(INFO) << space << node->to_string();

//      if(node->type() == LEAFNODE) {
//        auto leaf_node = (LeafNode *)node;
//        if (leaf_node->prev != nullptr)
//          CHECK(leaf_node->prev->next == leaf_node) << "Left brother dosen't point to me";
//        if (leaf_node->next != nullptr)
//          CHECK(leaf_node->next->prev == node) << "Right brother dosen't point to me";
//      }
      if (node->type() == INNERNODE) {
        InnerNode *inode = (InnerNode *)node;
        for (int i = 0; i < inode->slot_used; i++) {
          auto child = inode->values[i];
          CHECK(child->parent == node) << "My child " << i << " does not point to me";
        }
      }
      if (node->type() == INNERNODE) {
        InnerNode *inode = (InnerNode *)node;
        for (int i = 0; i < inode->slot_used; i++) {
          auto child = inode->values[i];
          KeyType *key_set;
          if (child->type() == LEAFNODE)
            key_set = ((LeafNode *)child)->keys;
          else
            key_set = ((InnerNode *)child)->keys;
          if (child->slot_used == 0) {
            CHECK(node == tree_root) << "Non root node has empty child " << i;
          } else {
            int idx = 0;
            for (int j = 1; j < child->slot_used; j++) {
              if (key_less(key_set[j], key_set[idx])) {
                idx = j;
              }
            }

            auto child_min_key = key_set[idx];
            if(child->type() == INNERNODE) {
              CHECK(idx == 0) << "InnerNode " << i << "'s first key isn't the smallest";
            }
            CHECK(!key_less(child_min_key, inode->keys[i])) << "My child " << i << " is beyond the key range";
          }
        }

        for (int i = 0; i < inode->slot_used; i++) {
          ensure_tree_structure(inode->values[i], indent + 4);
        }
      }
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
        done_(false) {
          // Initialize 2 layers of modifications
          node_mods_.push_back(NodeModsMapType());
          node_mods_.push_back(NodeModsMapType());
        }
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

        int sleep_time = 0;
        while (palmtree_->current_batch_.size() != BATCH_SIZE) {
          TreeOp *op = nullptr;
          bool res = palmtree_->task_queue_.pop(op);
          if (res) {
            palmtree_->current_batch_.push_back(op);
          } else
            boost::this_thread::sleep_for(boost::chrono::milliseconds(10));
          sleep_time += 1;
          if (sleep_time > 3)
            break;
        }

        DLOG(INFO) << "Collected a batch of " << palmtree_->current_batch_.size();
        if (palmtree_->current_batch_.size() == 0)
          return;

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
          if (result.find(op->target_node_) == result.end()) {
            result.emplace(op->target_node_, std::vector<TreeOp *>());
          }
          result[op->target_node_].push_back(op);
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

        DLOG(INFO) << "Worker " << worker_id_ << " has " << result.size() << " nodes of tasks after task redistribution";
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
        node_mods_[0].clear();
        auto &leaf_mods = node_mods_[0];
        std::unordered_map<KeyType, ValueType> changed_values;
        std::unordered_set<KeyType> deleted;
        for (auto itr = tree_ops.begin(); itr != tree_ops.end(); itr++) {
          LeafNode *leaf = static_cast<LeafNode *>(itr->first);
          auto &ops = itr->second;
          for (auto op : ops) {
            if (op->op_type_ == TREE_OP_FIND) {
              if (deleted.find(op->key_) != deleted.end()) {
                op->boolean_result_ = false;
              } else {
                if (changed_values.count(op->key_) != 0) {
                  op->result_ = changed_values[op->key_];
                  op->boolean_result_ = true;
                } else {
                  int idx = palmtree_->search_helper(leaf->keys, leaf->slot_used, op->key_);
                  if (idx == -1 || !palmtree_->key_eq(leaf->keys[idx], op->key_)) {
                    // Not find
                    op->boolean_result_ = false;
                  } else {
                    op->result_ = leaf->values[idx];
                    op->boolean_result_ = true;
                  }
                }
              }
            } else if (op->op_type_ == TREE_OP_INSERT) {
              DLOG(INFO) << "Try to insert " << op->key_ << ": " << op->value_;
              deleted.erase(op->key_);
              changed_values[op->key_] = op->value_;
              if (leaf_mods.count(leaf) == 0)
                leaf_mods.emplace(leaf, std::vector<NodeMod>());
              leaf_mods[leaf].push_back(NodeMod(*op));
            } else {
              CHECK(op->op_type_ == TREE_OP_REMOVE) << "Invalid tree operation";
              changed_values.erase(op->key_);
              if (leaf_mods.count(leaf) == 0)
                leaf_mods.emplace(leaf, std::vector<NodeMod>());
              leaf_mods[leaf].push_back(NodeMod(*op));
            }
          }
        }
      }

      /**
       * @brief Handle root split and re-insert orphaned keys. It may need to grow the tree height
       */
      void handle_root() {
        int root_depth = palmtree_->tree_depth_;
        std::vector<NodeMod> root_mods;
        // Collect root modifications from all threads
        for (auto &wthread : palmtree_->workers_) {
          auto itr = wthread.node_mods_[root_depth].begin();
          if (itr != wthread.node_mods_[root_depth].end()) {
            root_mods.insert(root_mods.end(), itr->second.begin(), itr->second.end());
          }
        }

        // Handle over to modify_node
        auto new_mod = palmtree_->modify_node(palmtree_->tree_root, root_mods);
        if (new_mod.type_ == MOD_TYPE_NONE) {
          DLOG(INFO) << "Root won't split";
        } else if (new_mod.type_ == MOD_TYPE_ADD) {
          DLOG(INFO) << "Split root";
          InnerNode *new_root = new InnerNode(nullptr);
          palmtree_->tree_root->parent = new_root;
          palmtree_->add_item<InnerNode, Node *>(new_root, palmtree_->min_key_, palmtree_->tree_root);
          for (auto itr = new_mod.node_items.begin(); itr != new_mod.node_items.end(); itr++) {
            itr->second->parent = new_root;
            palmtree_->add_item<InnerNode, Node *>(new_root, itr->first, itr->second);
          }
          palmtree_->tree_root = new_root;
          palmtree_->tree_depth_ += 1;
          for (auto &wthread : palmtree_->workers_) {
             wthread.node_mods_.push_back(NodeModsMapType());
          }
        }
        // Merge root if neccessary
        if (palmtree_->tree_depth_ >= 2 && palmtree_->tree_root->slot_used == 1) {
          DLOG(INFO) << "Decrease tree depth";
          // Decrease root height
          auto old_root = static_cast<InnerNode *>(palmtree_->tree_root);
          palmtree_->tree_root = old_root->values[0];
          delete old_root;
          palmtree_->tree_depth_ -= 1;
          for (auto &wthread : palmtree_->workers_) {
             wthread.node_mods_.pop_back();
          }
        }
        DLOG(INFO) << "Insert orphaned";
        // Naively insert orphaned
        for (auto itr = new_mod.orphaned_kv.begin(); itr != new_mod.orphaned_kv.end(); itr++) {
          DLOG(INFO) << "Insert " << itr->first << " " << itr->second;
          auto leaf = palmtree_->search(itr->first);
          palmtree_->add_item<LeafNode, ValueType>(leaf, itr->first, itr->second);
        }
        DLOG(INFO) << "Root handled";
      }

      // Worker loop: process tasks
      void worker_loop() {
        while (!done_) {
          // Stage 0, collect work batch and partition
          DLOG_IF(INFO, worker_id_ == 0) << "#### STAGE 0: collect tasks";
          if (worker_id_ == 0) {
            collect_batch();
          }
          palmtree_->sync();
          DLOG_IF(INFO, worker_id_ == 0) << "#### STAGE 0 finished";
          // Stage 1, Search for leafs
          DLOG(INFO) << "Worker " << worker_id_ << " got " << current_tasks_.size() << " tasks";
          DLOG_IF(INFO, worker_id_ == 0) << "#### STAGE 1: search for leaves";
          for (auto op : current_tasks_) {
            op->target_node_ = palmtree_->search(op->key_);
          }
          palmtree_->sync();
          DLOG_IF(INFO, worker_id_ == 0) << "#### STAGE 1 finished";
          DLOG_IF(INFO, worker_id_ == 0) << "#### STAGE 2: Process leaves";
          // Stage 2, redistribute work, read the tree then modify, each thread
          // will handle the nodes it has searched for, except the nodes that
          // have been handled by workers whose worker_id is less than me.
          // Currently we use a unordered_map to record the ownership of tasks upon
          // certain nodes.
          std::unordered_map<Node *, std::vector<TreeOp *>> collected_tasks;
          redistribute_leaf_tasks(collected_tasks);
          resolve_hazards(collected_tasks);
          DLOG_IF(INFO, worker_id_ == 0) << "resolved hazards";
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
            DLOG(INFO) << "Add node modification " << upper_mod.type_ << " to upper layer " << 1;
            if (upper_mods.find(node->parent) == upper_mods.end()) {
              upper_mods.emplace(node->parent, std::vector<NodeMod>());
            }
            upper_mods[node->parent].push_back(upper_mod);
          }
          palmtree_->sync();
          DLOG_IF(INFO, worker_id_ == 0) << "#### STAGE 2 finished";
          DLOG_IF(INFO, worker_id_ == 0) << "#### STAGE 3: propagate tree modification";
          // Stage 3, propagate tree modifications back
          // Propagate modifications until root
          for (int layer = 1; layer <= palmtree_->tree_depth_-1; layer++) {
            DLOG_IF(INFO, worker_id_ == 0) << "Layer #" << layer << " begin";
            NodeModsMapType cur_mods;
            redistribute_inner_tasks(layer, cur_mods);
            auto &upper_mods = node_mods_[layer+1];
            upper_mods.clear();
            for (auto itr = cur_mods.begin(); itr != cur_mods.end(); itr++) {
              auto node = itr->first;
              auto &mods = itr->second;
              DLOG(INFO) << "Stage 3 modify " << node->id;
              auto mod_res = palmtree_->modify_node(node, mods);
              if (upper_mods.count(node->parent) == 0) {
                upper_mods.emplace(node->parent, std::vector<NodeMod>());
              }
              upper_mods[node->parent].push_back(mod_res);
            }
            palmtree_->sync();
            DLOG_IF(INFO, worker_id_ == 0) << "Layer #" << layer << " done";
          } // End propagate
          DLOG_IF(INFO, worker_id_ == 0) << "#### STAGE 3 finished";
          DLOG_IF(INFO, worker_id_ == 0) << "#### STAGE 4: Handle root";
          // Stage 4, modify the root, re-insert orphaned, mark work as done
          if (worker_id_ == 0) {
            // Mark tasks as done
            handle_root();
            for (auto &wthread : palmtree_->workers_) {
             for (auto op : wthread.current_tasks_) {
               op->done_ = true;
             }
             wthread.current_tasks_.clear();
            }
          }
          palmtree_->ensure_tree_structure(palmtree_->tree_root, 0);
          DLOG_IF(INFO, worker_id_ == 0) << "#### STAGE 4 finished";
        } // End worker loop
      }
    }; // End WorkerThread

    std::vector<WorkerThread> workers_;
    /**********************
     * PalmTree public    *
     * ********************/
  public:
    PalmTree(KeyType min_key): tree_depth_(1), min_key_(min_key), barrier_{NUM_WORKER}, task_queue_{10240} {
      init();
    }

    void init() {
      // Init the root node
      tree_root = new InnerNode(nullptr);
      add_item<InnerNode, Node *>((InnerNode *)tree_root, min_key_, new LeafNode(tree_root));
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
          free_recursive(ptr->values[i]);
        }
      }
      delete node;
    }

    ~PalmTree() {
      DLOG(INFO) << "Destroy palm tree";
      for (auto &worker : workers_) {
        worker.exit();
      }
      free_recursive(tree_root);
    }

    /**
     * @brief Find the value for a key
     * @param key the key to be retrieved
     * @return nullptr if no such k,v pair
     */
    bool find(const KeyType &key UNUSED, ValueType &value) {
      TreeOp op(TREE_OP_FIND, key);
      task_queue_.push(&op);

      op.wait();
      if (op.boolean_result_)
        value = op.result_;
      return op.boolean_result_;
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
  // template class PalmTree<int, int>;
} // End of namespace palmtree

