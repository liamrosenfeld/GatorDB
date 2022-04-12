import unittest

from gdb_bplustree import BPlusTree


class TreeTests(unittest.TestCase):
    def test_insert(self):
        tree = BPlusTree(4)
        tree.insert(1, "one")
        tree.insert(2, "two")
        tree[3] = "three"
        tree[4] = "four"

        self.assertEqual(
            list(tree), [(1, "one"), (2, "two"), (3, "three"), (4, "four")]
        )

    def test_replace(self):
        tree = BPlusTree(4)

        # initial insert
        tree.insert(1, "one")
        tree[2] = "two"
        self.assertEqual(list(tree), [(1, "one"), (2, "two")])

        # replace
        tree.insert(1, "oneee")
        tree[2] = "twooo"
        self.assertEqual(list(tree), [(1, "oneee"), (2, "twooo")])

    def test_split(self):
        tree = BPlusTree(4)
        for i in range(0, 9):
            tree.insert(i, "value")

        out = tree.display()
        self.assertEqual(
            out,
            (
                "└─ [2, 4, 6]\n"
                "   ├─ [0, 1]\n"
                "   ├─ [2, 3]\n"
                "   ├─ [4, 5]\n"
                "   └─ [6, 7, 8]"
            ),
        )

    def test_get(self):
        tree = BPlusTree(4)
        for i in range(0, 9):
            tree.insert(i, f"value{i}")

        self.assertEqual(tree.get(3), tree[3], "value3")


if __name__ == "__main__":
    unittest.main()
