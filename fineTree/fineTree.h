//
// Created by Zrs_y on 5/5/16.
//

#ifndef FINETREE_FINETREE_H
#define FINETREE_FINETREE_H

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
  }

  int search(KeyType UNUSED key) {
    return 0;
  }

  void insert(KeyType UNUSED key, ValueType UNUSED val) {

  }

private:
  KeyType min_key;
  // Max number of slots per inner node
  static const int INNER_MAX_SLOT = 256;
  // Max number of slots per leaf node
  static const int LEAF_MAX_SLOT = 64;

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

};


#endif //FINETREE_FINETREE_H
