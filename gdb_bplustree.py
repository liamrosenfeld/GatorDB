import random


class Node:

    def __init__(self, parent=None):
        self.keys = []
        self.values: [Node] = []
        self.parent = parent

    def properIdx(self, key):
        """Returns the index where that key belongs"""
        for idx, item in enumerate(self.keys):
            if key < item:
                return idx
        return len(self.keys)

    def set(self, key, value):
        i = self.properIdx(key)
        self.keys[i:i] = [key]
        self.values.pop(i)
        self.values[i:i] = value

    def get(self, key):
        return self.values[self.properIdx(key)]

    def split(self):
        """split the node into two and store them as children of the current parent
        returns the key for the two children and both children
        """
        left = Node(self.parent)
        mid = len(self.keys) // 2

        left.keys = self.keys[:mid]
        left.values = self.values[:mid + 1]

        for child in left.values:
            child.parent = left

        key = self.keys[mid]
        self.keys = self.keys[mid + 1:]
        self.values = self.values[mid + 1:]

        return key, [left, self]


class Leaf(Node):
    def __init__(self, parent: Node = None, prev: Node = None, next: Node = None):
        super(Leaf, self).__init__(parent)
        self.prev: Leaf = prev
        self.next: Leaf = next

        # set prev and next of surrounding
        if next is not None:
            next.prev = self
        if prev is not None:
            prev.next = self

    def get(self, key):
        return self.values[self.keys.index(key)]

    def set(self, key, value):
        idx = self.properIdx(key)
        if key not in self.keys:
            # add new key value pair where it belongs
            self.keys.insert(idx, key)
            self.values.insert(idx, value)
        else:
            # update existing value of key
            self.values[idx - 1] = value

    def split(self):
        # create new node
        left = Leaf(self.parent, self.prev, self)
        self.prev = left

        # set left node to half of the current node's values
        mid = len(self.keys) // 2
        left.keys = self.keys[:mid]
        left.values = self.values[:mid]

        # drop the first half of the current node't value
        self.keys = self.keys[mid:]
        self.values = self.values[mid:]

        # key that divides the two new nodes is the leftmost key of the right leaf
        parentKey = self.keys[0]

        return parentKey, [left, self]


class BPlusTree:

    def __init__(self, order: int = 100):
        self.root = Leaf()
        self.order = order

    # --------- public ---------
    def insert(self, key, value):
        leaf = self.find(key)
        leaf.set(key, value)

        # if greater than order,
        # will need to split and then insert that into the tree
        if len(leaf.keys) > self.order:
            self.insert_from_split(*leaf.split())

    def get(self, key) -> Node:
        leaf = self.find(key)
        if key in leaf.keys:
            return leaf.get(key)
        else:
            return None

    def display(self, node=None, _prefix="", _last=True):
        if node is None:
            node = self.root

        print(_prefix, "└─ " if _last else "├─ ", node.keys, sep="")

        _prefix += "   " if _last else "│  "

        if type(node) is Node:
            for i, child in enumerate(node.values):
                _last = (i == len(node.values) - 1)
                self.display(child, _prefix, _last)

    # --------- sugar support ---------
    def __iter__(self):
        """Iterates over (key, value) of each leaf node"""

        curr = self.leftmost_leaf()

        while True:
            for elem in zip(curr.keys, curr.values):
                yield elem

            if (curr.next != None):
                curr = curr.next
            else:
                break

    def __setitem__(self, key, value):
        self.insert(key, value)

    def __getitem__(self, key):
        return self.get(key)

    # --------- internal ---------
    def insert_from_split(self, key, values: [Node]):
        parent: Node = values[0].parent

        if parent is None:
            # the top node just split, so we need to create a new root
            self.root = Node()
            values[0].parent = self.root
            values[1].parent = self.root

            self.root.keys = [key]
            self.root.values = values
            return

        parent.set(key, values)

        # if the the parent is now full, we need to do this whole things over again
        if len(parent.keys) > self.order:
            self.insert_from_split(*parent.split())

    def find(self, key) -> Leaf:
        """finds the leaf that should contain that key"""
        node = self.root
        # keep traversing until you hit a leaf
        while type(node) is not Leaf:
            node = node.get(key)
        return node

    def leftmost_leaf(self) -> Leaf:
        node = self.root
        while type(node) is not Leaf:
            node = node.values[0]
        return node

# ----------------------------------- testing ----------------------------------------- #


def demo():
    tree = BPlusTree(5)
    random_list = random.sample(range(1, 100), 30)
    for i in random_list:
        tree.insert(i, f"test{i}")
        print(f"Insert {i}")
        tree.display()
    for i in tree:
        print(i)
    print(random_list[0], tree.get(random_list[0]))
    print(tree.get(300))


if __name__ == "__main__":
    demo()
