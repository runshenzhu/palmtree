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
    struct Node {
      // Number of actually used slots
      int slot_used;

      Node(){};
    };

    struct InnerNode : public Node {
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

    struct LeafNode : public Node {
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

    template <typename node_type>
    // Return the index of the first slot whose key >= @key
    inline int find_lower(const Node* n, const KeyType &key) const
    {
      const int BIN_SEARCH_THRESHOLD = 128;
      if (sizeof(n->keys) > BIN_SEARCH_THRESHOLD)
      {
        if (n->slot_used == 0) return 0;

        int lo = 0, hi = n->slotuse;

        while (lo < hi)
        {
          int mid = (lo + hi) >> 1;

          if (Compare(key, n->keys[mid])) {
            hi = mid;     // key <= mid
          }
          else {
            lo = mid + 1; // key > mid
          }
        }

        return lo;
      }
      else // for nodes <= binsearch_threshold do linear search.
      {
        int lo = 0;
        while (lo < n->slotuse && key_less(n->slotkey[lo], key)) ++lo;
        return lo;
      }
    }

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
