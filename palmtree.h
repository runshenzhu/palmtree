#pragma once

#include <functional>
#include <vector>
#include <assert.h>

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

  private:
    /**
     * Tree node base class
     */
    struct Node {
      // Number of actually used slots
      int slot_used;

      Node(){};
      virtual NodeType Type() const = 0;
    };

    struct InnerNode : public Node {
      InnerNode(){};
      // Keys for children
      KeyType keys[INNER_MAX_SLOT];
      // Pointers for children
      Node *children[INNER_MAX_SLOT];

      virtual NodeType Type() const {
        return INNERNODE;
      }
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

      virtual NodeType Type() const {
        return LEAFNODE;
      }

      inline bool IsFull() const {
        return Node::slot_used == LEAF_MAX_SLOT;
      }

      inline bool IsFew() const {
        return Node::slot_used < LEAF_MAX_SLOT/4;
      }
    };
    /**
     * Tree operation wrappers
     */
    struct TreeOp {
      TreeOp(TreeOpType op_type, const KeyType &key, const ValueType &value):
        op_type_(op_type), key_(key), value_(value) {};
      TreeOp(TreeOpType op_type, const KeyType &key):
        op_type_(op_type), key_(key) {};

      TreeOpType op_type_;
      KeyType key_;
      ValueType value_;

      LeafNode *target_node_;
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
      // Leaf
      std::vector<std::pair<KeyType, ValueType>> value_items;
      // Inner
      std::vector<std::pair<KeyType, Node *>> node_items;
      std::vector<KeyType> orphaned_keys;
    };

  /********************
   * PalmTree private
   * ******************/
  private:
    Node *tree_root;

    KeyComparator kcmp;

    inline bool key_less(const KeyType &k1, const KeyType &k2) {
      return kcmp(k1, k2);
    }

    inline bool key_eq(const KeyType &k1, const KeyType &k2) {
      return !kcmp(k1, k2) && !kcmp(k2, k1);
    }
    // Return the index of the smallest slot whose key <= @key
    // assume there is no duplicated element
    int SearchHelper(const KeyType *input, int size, const KeyType &target) {
      int res = size;
      // loop all element
      for (int i = 0; i < size; i++) {
        if(key_less(target, input[i])){
          // target < input
          // ignore
          continue;

        }
        if (res == size || key_less(input[i], input[res])) {
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
      auto ptr = (InnerNode *)tree_root;
      for (;;) {
        auto idx = this->SearchHelper(ptr->keys, ptr->slot_used, key);
        Node *child = ptr->children[idx];
        if (child->Type() == LEAFNODE) {
          return (LeafNode *)child;
        }else {
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
      return NodeMod(MOD_TYPE_NONE);
    }

    /**************************
     * Concurrent executions **
     * ************************/

    /**********************
     * PalmTree public    *
     * ********************/
  public:
    PalmTree() {
      tree_root = new InnerNode();
    };

    ValueType *Find(const KeyType &key UNUSED) {
      return nullptr;
    }

    void Insert(const KeyType &key UNUSED, const ValueType &value UNUSED) {
      return;
    }

    void Remove(const KeyType &key UNUSED) {

    }
  }; // End of PalmTree
  template class PalmTree<int, int>;
} // End of namespace palmtree

