//
// Created by Zrs_y on 5/5/16.
//

#ifndef FINETREE_FINETREE_H
#define FINETREE_FINETREE_H

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <glog/logging.h>
#include <atomic>
#include "immintrin.h"

#define UNUSED __attribute__((unused))

enum NodeType {
  INNERNODE = 0,
  LEAFNODE
};

static std::atomic<int> NODE_NUM(0);

template <typename KeyType,
  typename ValueType,
  typename PairType = std::pair<KeyType, ValueType>,
  typename KeyComparator = std::less<KeyType> >
class fineTree {

public:
  fineTree(KeyType min_key) {
    this->min_key = min_key;
    root = new InnerNode(nullptr, 1);
    auto child = new LeafNode(root, 0);

    add_item_inner(root, min_key, child);

  }

  int search(KeyType UNUSED key, ValueType &res) {
    auto ptr = (InnerNode *)root;
    ptr->lock_shared();
    for (;;) {
      CHECK(ptr->slot_used > 0) << "Search empty inner node";

      auto idx = this->search_inner(ptr->keys, ptr->slot_used, key);
      CHECK(idx != -1) << "search innerNode fail";
      CHECK(key_less(ptr->keys[idx], key) || key_eq(ptr->keys[idx], key));
      if(idx + 1 < ptr->slot_used) {
        CHECK(key_less(key, ptr->keys[idx + 1]));
      }
      auto child = ptr->values[idx];
      child->lock_shared();
      ptr->unlock_shared();
      if (child->type() == LEAFNODE) {
        auto leaf = (LeafNode *)child;
        idx = search_leaf(leaf->keys, leaf->slot_used, key);
        if (idx < 0) {
          child->unlock_shared();
          return -1;
        }else{
          res = leaf->values[idx];
          child->unlock_shared();
          return 0;
        }
      } else {
        ptr = (InnerNode *)child;
      }
    }


    return 0;
  }

  void insert(KeyType UNUSED key, ValueType UNUSED val) {
    root->lock_exclusive();

    auto new_child = insert_inner((InnerNode *)root, key, val);

    if(new_child == nullptr) {
      root->unlock_execlusive();
      return;
    }

    auto new_root = new InnerNode(nullptr, root->level + 1);
    root->parent = new_root;
    new_child->parent = new_root;

    add_item_inner(new_root, root->keys[0], root);
    add_item_inner(new_root, ((InnerNode *)new_child)->keys[0], new_child);

    root = new_root;
    root->values[0]->unlock_execlusive();
  }

  void test() {
    root->upgrade_lock();
    std::cout << "lock shared" << std::endl;
    root->upgrade_lock();
    std::cout << "lock exclusive" << std::endl;
  }

private:
  KeyType min_key;
  // Max number of slots per inner node
  static const int INNER_MAX_SLOT = 64;
  // Max number of slots per leaf node
  static const int LEAF_MAX_SLOT = 128;

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


  struct Node {
    // Number of actually used slots
    int slot_used;
    int id;
    int level;
    KeyType lower_bound;
    Node *parent;


    Node() = delete;
    Node(Node *p, int lvl): slot_used(0), level(lvl), parent(p) {
      id = NODE_NUM++;
    };

    void lock_shared() {
      lock.lock();
    }

    void unlock_shared() {
      lock.unlock();
    }

    void lock_exclusive() {
      lock.lock();
    }

    void unlock_execlusive() {
      lock.unlock();
    }

    // upgrade to exclusive lock
    void upgrade_lock() {
      lock.lock();
    }

    // downgrade to shared lock
    void downgrade_lock() {
      lock.unlock();
    }


    // boost::upgrade_mutex lock;
    spinlock lock;
    virtual ~Node() {};
    virtual std::string to_string() = 0;
    virtual NodeType type() const = 0;
    virtual bool is_few() = 0;
  };

  struct InnerNode : public Node {
    InnerNode() = delete;
    InnerNode(Node *parent, int level): Node(parent, level){};
    virtual ~InnerNode() {};
    // Keys for values
    KeyType keys[LEAF_MAX_SLOT];
    // Pointers for child nodes
    Node *values[LEAF_MAX_SLOT];

    virtual NodeType type() const {
      return INNERNODE;
    }

    virtual std::string to_string() {
      std::string res;
      res += "InnerNode[" + std::to_string(Node::id) + " @ " + std::to_string(Node::level) + "] ";
      // res += std::to_string(Node::slot_used);
      for (int i = 0 ; i < Node::slot_used ; i++) {
        res += " " + std::to_string(keys[i]) + ":" + std::to_string(values[i]->id);
      }
      return res;
    }

    inline bool is_full() const {
      return Node::slot_used == MAX_SLOT();
    }


    inline size_t MAX_SLOT() const {
      return LEAF_MAX_SLOT;
    }

    virtual inline bool is_few() {
      return Node::slot_used < MAX_SLOT()/4 || Node::slot_used == 0;
    }

  };

  struct LeafNode : public Node {
    LeafNode() = delete;
    LeafNode(Node *parent, int level): Node(parent, level){};
    virtual ~LeafNode() {};

    // Keys and values for leaf node
    KeyType keys[INNER_MAX_SLOT];
    ValueType values[INNER_MAX_SLOT];

    virtual NodeType type() const {
      return LEAFNODE;
    }

    virtual std::string to_string() {
      std::string res;
      res += "LeafNode[" + std::to_string(Node::id) + " @ " + std::to_string(Node::level) + "] ";

      for (int i = 0 ; i < Node::slot_used ; i++) {
        res += " " + std::to_string(keys[i]) + ":" + std::to_string(values[i]);
      }
      return res;
    }

    inline bool is_full() const {
      return Node::slot_used == MAX_SLOT();
    }

    inline size_t MAX_SLOT() const {
      return INNER_MAX_SLOT;
    }

    virtual inline bool is_few() {
      return Node::slot_used < MAX_SLOT()/4 || Node::slot_used == 0;
    }
  };

  // Return true if k1 < k2
  inline bool key_less(const KeyType &k1, const KeyType &k2) {
    return k1 < k2;
  }
  // Return true if k1 == k2
  inline bool key_eq(const KeyType &k1, const KeyType &k2) {
    return k1 == k2;
  }

  // Return the index of the largest slot whose key <= @target
  // assume there is no duplicated element
  int search_inner(const KeyType *input, int size, const KeyType &target) {
    // auto bt = CycleTimer::currentTicks();
    int low = 0, high = size - 1;
    while (low != high) {
      int mid = (low + high) / 2 + 1;
      if (key_less(target, input[mid])) {
        // target < input[mid]
        high = mid - 1;
      }
      else {
        // target >= input[mid];
        low = mid;
      }
    }
    // STAT.add_stat(0, "search_inner", CycleTimer::currentTicks() - bt);

    if (low == size) {
      return -1;
    }
    return low;
  }

  int search_leaf(const KeyType *data, int size, const KeyType &target) {
    // auto bt = CycleTimer::currentTicks();
    const __m256i keys = _mm256_set1_epi32(target);

    const auto n = size;
    const auto rounded = 8 * (n/8);

    for (int i=0; i < rounded; i += 8) {

      const __m256i vec1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&data[i]));

      const __m256i cmp1 = _mm256_cmpeq_epi32(vec1, keys);

      const uint32_t mask = _mm256_movemask_epi8(cmp1);

      if (mask != 0) {
        // STAT.add_stat(0, "search_leaf", CycleTimer::currentTicks() - bt);
        return i + __builtin_ctz(mask)/4;
      }
    }

    for (int i = rounded; i < n; i++) {
      if (data[i] == target) {
        // STAT.add_stat(0, "search_leaf", CycleTimer::currentTicks() - bt);
        return i;
      }
    }

    // STAT.add_stat(0, "search_leaf", CycleTimer::currentTicks() - bt);
    return -1;
  }



  void add_item_leaf(LeafNode *node, KeyType key, ValueType value) {
    auto idx = node->slot_used++;
    node->keys[idx] = key;
    node->values[idx] = value;
    return;
  }

  void add_item_inner(InnerNode *node, KeyType key, Node *value) {
    // add item to inner node
    // ensure it's order

    if(node->slot_used == 0) {
      node->keys[0] = key;
      node->values[0] = value;
      node->slot_used++;
      return;
    }

    auto idx = search_inner(node->keys, node->slot_used, key);

    CHECK(idx != -1) << "search innerNode fail" << key <<" " <<node->keys[0];
    CHECK(key_less(node->keys[idx], key) || key_eq(node->keys[idx], key));
    if(idx + 1 < node->slot_used) {
      CHECK(key_less(key, node->keys[idx + 1])) << "search inner fail";
    }

    auto k = key;
    auto v = value;

    for(int i = idx + 1; i < node->slot_used; i++) {
      std::swap(node->keys[i], k);
      std::swap(node->values[i], v);
    }

    node->keys[node->slot_used] = k;
    node->values[node->slot_used] = v;
    node->slot_used++;
  }

  Node *insert_leaf(LeafNode *node, KeyType key, ValueType value) {
    // assume we hold the exclusive lock of leaf


    // node not full
    // simple add item to leaf
    if(!node->is_full()) {
      add_item_leaf(node, key, value);
      return nullptr;
    }

    // otherwise, firstly buff all elements
    std::vector<std::pair<KeyType, ValueType>> buff;
    for(int i = 0; i < node->slot_used; i++) {
      buff.push_back(std::make_pair(node->keys[i], node->values[i]));
    }
    buff.push_back(std::make_pair(key, value));


    // sort
    std::sort(buff.begin(), buff.end(), [this](const std::pair<KeyType, ValueType> &p1, const std::pair<KeyType, ValueType> &p2) {
      return key_less(p1.first, p2.first);
    });


    // split into 2 parts
    // store the second half to new node
    auto half = buff.size() / 2;
    auto itr = buff.begin();
    node->slot_used = 0;
    for(int i = 0; i < half; i++) {
      add_item_leaf(node, itr->first, itr->second);
      itr++;
    }

    auto new_child = new LeafNode(node->parent, 0);

    while(itr != buff.end()) {
      add_item_leaf(new_child, itr->first, itr->second);
      itr++;
    }


    // return the new node to upper layer
    return new_child;
  }

  Node *insert_inner(InnerNode *node, KeyType key, ValueType value) {
    // assume we hold the exclusive lock before entering this function

    // firstly, find the child to insert
    auto idx = search_inner(node->keys, node->slot_used, key);
    CHECK(idx != -1)  << "search fail";
    auto child = node->values[idx];
    Node *new_child = nullptr;
    if(child->type() == LEAFNODE) {
      child->lock_exclusive();
      new_child = insert_leaf((LeafNode *)child, key, value);
    }else {
      // child->lock_shared();
      child->lock_exclusive();
      new_child = insert_inner((InnerNode *)child, key, value);
    }

    // child not split
    if(new_child == nullptr) {
      child->unlock_execlusive();
      return nullptr;
    }

    // child split
    KeyType new_key;
    if(new_child->type() == LEAFNODE) {
      new_key = ((LeafNode *)new_child)->keys[0];
    }else{
      new_key = ((InnerNode *)new_child)->keys[0];
    }

    // node not split
    if(!node->is_full()) {
      add_item_inner(node, new_key, new_child);
      child->unlock_execlusive();
      return nullptr;
    }

    // node also need split

    // lock all children
    for(int i = 0; i < node->slot_used; i++) {
      if(node->values[i] != child) {
        node->values[i]->lock_exclusive();
      }
    }


    // buff all elements
    std::vector<std::pair<KeyType, Node *>> buff;
    for(int i = 0; i < node->slot_used; i++) {
      buff.push_back(std::make_pair(node->keys[i], node->values[i]));
    }
    buff.push_back(std::make_pair(new_key, new_child));


    // sort
    std::sort(buff.begin(), buff.end(), [this](const std::pair<KeyType, Node *> &p1, const std::pair<KeyType, Node *> &p2) {
      return key_less(p1.first, p2.first);
    });


    // store half
    auto half = buff.size() / 2;
    auto itr = buff.begin();
    node->slot_used = 0;
    for(int i = 0; i < half; i++) {
      node->keys[i] = itr->first;
      node->values[i] = itr->second;
      node->slot_used++;
      itr++;
    }

    // new node store another half
    auto new_inner = new InnerNode(node->parent, node->level);

    int i = 0;
    while(itr != buff.end()) {
      new_inner->keys[i] = itr->first;
      new_inner->values[i] = itr->second;
      new_inner->slot_used++;
      itr->second->parent = new_inner;
      itr++;
      i++;
    }


    // unlock children
    for(int i = 0; i < node->slot_used; i++) {
      if(node->values[i] != new_child) {
        node->values[i]->unlock_execlusive();
      }
    }

    for(int i = 0; i < new_inner->slot_used; i++) {
      if(new_inner->values[i] != new_child) {
        new_inner->values[i]->unlock_execlusive();
      }
    }
    return new_inner;
  }

  InnerNode *root;

};


#endif //FINETREE_FINETREE_H
