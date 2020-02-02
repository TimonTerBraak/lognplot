import rocksdb
import struct
from ctypes import *
import unittest

class Node:

    def __bytes__(self):
        pass

    @classmethod
    def fromBytes(cls, count, data):
        pass 

class MetaNode(Node):

    """
        Signals is a dictionary with name -> root_id
    """
    def __init__(self, signals):
        self._signals = signals

    def __bytes__(self):
        meta = struct.pack('i', len(self._signals))
        for name, rootid in self._signals.items():
            fmt = "ii{}c".format(len(name))
            meta = meta.join(struct.pack(fmt, rootid, len(name), *name))
        return meta

    @classmethod
    def fromBytes(cls, count, data):
        signals = dict()
        offset = 4
        for i in range(0, count):
            l, = struct.unpack_from('i', offset)
            fmt = "ii{}c".format(l)
            signal = struct.unpack_from(fmt, data, offset)
            offset = offset + 4 + 4 + l
            signals[signal[1]] = signal[0]

class InternalNode(Node):

    def __init__(self, children, aggregation):
        self._children = children
        self._aggregation = aggregation

    def children(self):
        return self._children

    def aggregation(self):
        return self._aggregation

    def __bytes__(self):
        fmt = "ii{}i{}d".format(len(self._children), len(self._aggregation))
        return struct.pack(fmt, -int(len(self._children)), int(len(self._aggregation)), *self._children, *self._aggregation)

    @classmethod
    def fromBytes(cls, count, data):
        l, = struct.unpack_from('i', data, 4)
        fmt = "ii{}i{}d".format(count, l)
        plain = list(struct.unpack(fmt, data))
        return cls(plain[2:2+count], plain[2+count+1:])

class LeafNode(Node):

    def __init__(self, data):
        self._data = data

    def samples(self):
        return self._data

    def __bytes__(self):
        ls = len(self.samples())
        fmt = "i{}d".format(ls * 2)
        _samples = [x for y in self.samples() for x in y]
        return struct.pack(fmt, ls, *_samples)

    @classmethod
    def fromBytes(cls, count, data):
        fmt = "i{}d".format(count * 2)
        plain = list(struct.unpack(fmt, data))
        return cls(list(zip(plain[1::2], plain[2::2])))

"""
    NODE: key -> (count, children[], aggregation[])
    LEAF: key -> (count, samples[])
"""
class KeyValueStore:

    """
        Key 0 is reserved for metadata.
    """
    def __init__(self, filename):
        self._filename = filename
        self._options = rocksdb.Options(create_if_missing=True)
        self._db = rocksdb.DB(self._filename, self._options)
        self._batch = None

        metadata = self._db.get_live_files_metadata()
        if metadata:
            self._lastkey, = struct.unpack('i', metadata[0]['largestkey'])
        else:
            self._lastkey = 0

    def _nextkey(self):
        key = self._lastkey + 1
        self._lastkey = key
        return key

    def __enter__(self):
        self._batch = rocksdb.WriteBatch()
        return self

    def __exit__(self, *exit_args):
        self._db.write(self._batch)
        self._batch = None

    def set(self, key, node):
        if self._batch:
            self._batch.put(struct.pack('i', key), bytes(node))
        else:
            self._db.put(struct.pack('i', key), bytes(node))

    def add(self, node):
        key = self._nextkey()
        self.set(key, node)
        return key

    def get(self, key):
        data = self._db.get(struct.pack('i', key))
        count, = struct.unpack_from('i', data)
        if count <= 0:
            return InternalNode.fromBytes(-count, data)
        else:
            return LeafNode.fromBytes(count, data)

    def printTree(self, root):
        keys = [root]

        while len(keys) > 0:
            key, keys = keys[0], keys[1:]
            if key is not None:
                node = self.get(key)
                if isinstance(node, InternalNode):
                    print('node<{}>: {}, {}'.format(key, node.children(), node.aggregation()))
                    keys.extend(node.children())
                else:
                    print('leaf<{}>: {}'.format(key, node.samples())) 


class KeyValueStoreTest(unittest.TestCase):

    def _test_key_generation(self):
        store = KeyValueStore('test.db')
        for i in range(store._lastkey, store._lastkey + 100):
            self.assertEqual(i + 1, store._nextkey())

    def _test_simple_tree(self):
        store = KeyValueStore('test.db')
        leaf0 = store.add(LeafNode([(1,1), (2,2), (3,3)]))
        leaf1 = store.add(LeafNode([(4,4), (5,5), (6,6)]))
        node2 = store.add(InternalNode([leaf0, leaf1], [3.5, 1, 6]))
        store.printTree(node2)

    def _test_modify_tree(self):
        store = KeyValueStore('test.db')
        leaf0 = store.add(LeafNode([(1,1), (2,2), (3,3)]))
        leaf1 = store.add(LeafNode([(4,4), (5,5), (6,6)]))
        node2 = store.add(InternalNode([leaf0, leaf1], [3.5, 1, 6]))

        leaf3 = store.add(LeafNode([(7,7), (8,8), (9,9)]))
        leaf4 = store.add(LeafNode([(10,10), (11,11), (12,12)]))
        node5 = store.add(InternalNode([leaf3, leaf4], [8.5, 7, 12]))
        node6 = store.add(InternalNode([node2, node5], [6, 1, 12]))

        store.printTree(node6)

    def _test_large_database(self):
        store = KeyValueStore('test.db')
        leafNode = LeafNode([(1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9), (10,10)])
        leaf = store.add(leafNode)
        leafs = [leaf]
        for i in range(0, 100000):
            leaf = store.add(leafNode)
            leafs.append(leaf)
        node = store.add(InternalNode(leafs, [5, 1, 10]))
        #store.printTree(node)

    def test_batch_write(self):
        store = KeyValueStore('test.db')
        with store as transation:
            leafNode = LeafNode([(1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9), (10,10)])
            leaf = store.add(leafNode)
            leafs = [leaf]
            for i in range(0, 100000):
                leaf = store.add(leafNode)
                leafs.append(leaf)
            node = store.add(InternalNode(leafs, [5, 1, 10]))
        #store.printTree(node)



if __name__ == "__main__":
    unittest.main()
