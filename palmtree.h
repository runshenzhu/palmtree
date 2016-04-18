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
    static const int BIN_SEARCH_THRESHOLD = 32;
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

    // Return the index of the first slot whose key >= @key
    // assume there is no duplicated element
    int BSearch(const KeyType *input, int size, const KeyType &target) {
      if (size <= BIN_SEARCH_THRESHOLD) {
        // few elements, linear search
        int lo = 0;
        while (lo < size && Compare(input[lo], target)) ++lo;
        return lo;
      }


      int lo = 0, hi = size;
      while (lo != hi) {
        int mid = (lo + hi) / 2; // Or a fancy way to avoid int overflow
        if (Compare(input[mid], target)) {
          /* This index, and everything below it, must not be the first element
           * greater than what we're looking for because this element is no greater
           * than the element.
           */
          lo = mid + 1;
        }
        else {
          /* This element is at least as large as the element, so anything after it can't
           * be the first element that's at least as large.
           */
          hi = mid;
        }
      }
      /* Now, low and high both point to the element in question. */
      return lo;
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
