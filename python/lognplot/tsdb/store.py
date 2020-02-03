import abc
import rocksdb
import struct
from ctypes import *
import unittest

class TimeSeriesDatabase:

    def __init__(self, store: Store):
        self._store = store
        self._signals = dict()

        # Read metadata from the store, or create it if non-existent
        self._metadata = store.get(0)
        if not isinstance(self._metadata, EmptyNode):
            self._metadata = MetaNode([]) 
            self._store.set(0, self._metadata)

        for name, rootid in self._metadata.signals().items():
            self._signals[name] = Signal(self._store, name, rootid)

    def add(self, name = "") -> Signal:
        if name not in self._metadata:
            node = InternalNode([], [])
            rootid = self._store.add(node)
            signal = Signal(self._store, name, rootid) 
            self._metadata[name] = signal
            return signal
       else:
            raise ValueError

    def get(self, name) -> Signal:
        return self._metadata[name]

    def signals(self) -> dict():
        return self._signals

class Timespan:

    def __init__(self, step: float, count: int):
        pass

class Signal:

    def __init__(self, store, name, rootid, fan_out = 32):
        self._store = store
        self._name = name
        self._rootid = rootid
        self._fan_out = fan_out
        self._depth = 0
        self._ancestors = []
        self._modified = dict()
        self.samples = []
        self._open_tree()

    def _open_tree(self):
        self._ancestors = []
        key = self._rootid
        node = self._store.get(key)
        while isinstance(node, InternalNode):
            self._ancestors.append((key,node))
            key = node.children[-1]
            node = self._store.get(key)
            self._depth = self._depth + 1                

    # Adds a new layer of internals starting from the last ancestors, if any 
    def _get_parent(self) -> (int, Node):
        parent = None
        parent_key = 0

        # Traverse the tree upwards if nodes are completely filled
        while self._ancestors[-1][1].children() == self._fan_out:
            self._ancestors.pop()

        if len(self._ancestors) > 0:
            # We are 'halfway' in the tree where there is still some space.
            parent_key, parent = self._ancestors[-1]
            # This node will be modified
            self._mark_modified(parent_key, parent)
        else:
            # Complete filling, replace old root with new internal node,
            # increasing the capacity of the tree.
            root = self._store.get(self._rootid)
            new_key_for_old_root = self._add_later(root)
            parent = InternalNode([], [])
            parent.add_child(new_key_for_old_root)
            self._mark_modified(self._rootid, parent)
            # depth just increased by adding another level
            self._depth = self._depth + 1

        for n in range(0, self._depth - len(self._ancestors)):
            child = InternalNode([],[])
            parent_key = self._add_later(child)
            parent.add_child(key)
            parent = child

        return (parent_key, parent)

    def _add_later(self, node: Node) -> int:
        key = self._store.next_key()
        self._mark_modified(key, node)
        return key

    def _mark_modified(self, key: int, node: Node):
        self._modified[key] = node

    def _store_modifications(self):
        # Trigger all the modification to be updated in the store
        with self._store as transaction:
            for key, node in self._modified.items():
                self._store.set(key, node)
        self._modified = dict()

    def append(self, sample):
        self._samples.append(sample)
        if len(self._samples) == self._fan_out:
            parent = self._get_parent()
            leaf = LeafNode(self.samples)
            key = self._add_later(leaf)
            parent[1].add_child(key)
            self._mark_modified(*parent)
            self._store_modifications()

    def query(self):
        pass


class Node(metaclass=abc.ABCMeta): 

    META_NODE = 1
    INTERNAL_NODE = 2
    LEAF_NODE = 3

    @abc.abstractmethod
    def __bytes__(self) -> bytes:
        raise NotImplementedError

    @classmethod
    @abs.abstractmethod
    def from_bytes(cls, data: bytes) -> Node:
        raise NotImplementedError

    @classmethod
    def from_data(cls, data: bytes) -> Node:
        if data is None or len(data) == 0:
            return None
        nodetype, = struct.unpack_from('i', data)
        if nodetype == self.META_NODE:
            return MetaNode.from_bytes(data)
        elif nodetype == self.INTERNAL_NODE:
            return InternalNode.from_bytes(data)
        elif nodetype == self.LEAF_NODE:
            return LeafNode.from_bytes(data)
        else:
            raise NotImplementedError

class MetaNode(Node):

    """
        Signals is a dictionary with name -> root_id
    """
    def __init__(self, signals):
        self._signals = signals
        # TODO: add absolute time for t0, and timebase

    def signals(self):
        return self._signals

    def add_signal(self, name = "", root_id = -1):
        if name not in self._signals:
            self._signals[name] = root_id
            return True
        else:
            return False

    def __bytes__(self):
        meta = struct.pack('ii', len(self._signals))
        for name, rootid in self._signals.items():
            fmt = "ii{}c".format(len(name))
            meta = meta.join(struct.pack(fmt, self.META_NODE, rootid, len(name), *name))
        return meta

    @classmethod
    def from_bytes(cls, count, data):
        signals = dict()
        offset = 4
        for i in range(0, count):
            l, = struct.unpack_from('i', offset)
            fmt = "ii{}c".format(l)
            signal = struct.unpack_from(fmt, data, offset)
            offset = offset + 4 + 4 + l
            signals[signal[1]] = signal[0]
        # TODO: return 

class InternalNode(Node):

    def __init__(self, children, aggregation):
        self._children = children
        self._aggregation = aggregation

    def children(self):
        return self._children

    def aggregation(self):
        return self._aggregation

    def add_child(self, node: Node):
        self._children.append(node)
        # TODO: recalculate aggregation

    def __bytes__(self):
        fmt = "iii{}i{}d".format(len(self._children), len(self._aggregation))
        return struct.pack(fmt, self.INTERNAL_NODE, int(len(self._children)), int(len(self._aggregation)), *self._children, *self._aggregation)

    @classmethod
    def from_bytes(cls, data: bytes) -> Node:
        children, metrics = struct.unpack_from('ii', data)
        fmt = "ii{}i{}d".format(children, metrics)
        plain = list(struct.unpack(fmt, data))
        return cls(plain[2:2+count], plain[2+count+1:])

class LeafNode(Node):

    def __init__(self, data):
        self._data = data

    def samples(self):
        return self._data

    def __bytes__(self):
        ls = len(self.samples())
        fmt = "ii{}d".format(ls * 2)
        _samples = [x for y in self.samples() for x in y]
        return struct.pack(fmt, self.LEAF_NODE, ls, *_samples)

    @classmethod
    def from_bytes(cls, data: bytes) -> Node:
        count = len(data) - struct.calcsize('i')
        fmt = "i{}d".format(count)
        plain = list(struct.unpack(fmt, data))
        return cls(list(zip(plain[1::2], plain[2::2])))


class Store(metaclass=abc.ABCMeta) 

    def __init__(self):
        pass

    @abc.abstractmethod
    def get(self, key: int):
        raise NotImplementedError

    @abc.abstractmethod
    def set(self, key: int, node: Node):
        raise NotImplementedError 

    @abc.abstractmethod
    def add(self, node: Node) -> int:
        raise NotImplementedError 


class Dictionary(Store):

    def __init__(self):
        self._signals = dict()
        self._samples = dict()
        self._lastkey = 0

    def get(self, key: int):
        return Node.from_data(self._samples[key])

    def set(self, key: int, node: Node):
        self._samples[key] = bytes(node)

    def add(self, node: Node) -> int:
        self._lastkey = self._lastkey + 1
        key = self._lastkey
        self._samples[key] = bytes(node)


"""
    NODE: key -> (count, children[], aggregation[])
    LEAF: key -> (count, samples[])
"""
class KeyValueStore(Store):

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

    def next_key(self):
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
        key = self.next_key()
        self.set(key, node)
        return key

    def get(self, key):
        data = self._db.get(struct.pack('i', key))
        return Node.from_data(data)

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
            self.assertEqual(i + 1, store.next_key())

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
