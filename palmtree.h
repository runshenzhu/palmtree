#pragma once

#include <functional>

#define UNUSED __attribute__((unused))

namespace {

  template <typename KeyType,
           typename ValueType,
           typename PairType = std::pair<KeyType, ValueType>,
           typename Compare = std::less<KeyType> >
  class PalmTree {
    // Max number of slots per inner node
    static const int INNER_MAX_SLOT = 256;
    // Max number of slots per leaf node
    static const int LEAF_MAX_SLOT = 1024;



  private:
    class Node {
      // Number of actually used slots
      int slot_used;

      Node(){};
    };

    class InnerNode : public Node {
      InnerNode(){};
      // Keys for children
      KeyType keys[INNER_MAX_SLOT];
      // Pointers for children
      Node *children[INNER_MAX_SLOT+1];

      inline bool IsFull() const {
        return Node::slot_used == INNER_MAX_SLOT;
      }

      inline bool IsFew() const {
        return Node::slot_used < INNER_MAX_SLOT/2;
      }

    };

    class LeafNode : public Node {
      LeafNode(): prev(nullptr), next(nullptr) {};

      LeafNode *prev;

      LeafNode *next;

      KeyType keys[LEAF_MAX_SLOT];
      ValueType values[LEAF_MAX_SLOT];

      inline bool IsFull() const {
        return Node::slot_used == LEAF_MAX_SLOT;
      }

      inline bool IsFew() const {
        return Node::slot_used < LEAF_MAX_SLOT/4;
      }
    };

  /********************
   * PalmTree private
   * ******************/
  private:
    Node *tree_root;

  /**********************
   * PalmTree public
   * ********************/
  public:
    PalmTree() {
      tree_root = new Node();
    };

    ValueType *Find(const KeyType &key UNUSED) {
      return nullptr;
    }

    void Insert(const KeyType &key UNUSED, const ValueType &value UNUSED) {
      return;
    }

    void Remove(const KeyType &key UNUSED) {

    }
  };

}
