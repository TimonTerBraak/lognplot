import abc
import bisect
import rocksdb
import struct
import unittest
#from ..time import *
from lognplot.time.timespan import *
from lognplot.tsdb.aggregation import *
from lognplot.tsdb.metrics import *
from .storage import Store

class MWayTreeNode(metaclass=abc.ABCMeta):

    METADATA_NODE = 1
    INTERNAL_NODE = 2
    LEAF_NODE = 3

    @abc.abstractmethod
    def __bytes__(self) -> bytes:
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def from_bytes(cls, data: bytes):
        raise NotImplementedError()

    @classmethod
    def from_data(cls, data: bytes):
        if data is None or len(data) == 0:
            return None
        nodetype, = struct.unpack_from('i', data)
        if nodetype == cls.METADATA_NODE:
            return MWayTreeMetadataNode.from_bytes(data)
        elif nodetype == cls.INTERNAL_NODE:
            return MWayTreeInternalNode.from_bytes(data)
        elif nodetype == cls.LEAF_NODE:
            return MWayTreeLeafNode.from_bytes(data)
        else:
            raise NotImplementedError()

    @abc.abstractmethod
    def query_value(self, store, timestamp):
        raise NotImplementedError()

class MWayTreeMetadataNode(MWayTreeNode):

    """
        Forest is a dictionary with name -> root_id
    """
    def __init__(self):
        self._forest = dict()
        # TODO: add absolute time for t0, and timebase

    def __iter__(self):
        for name, root_id in self._forest.items():
            yield (name, root_id)

    def add_tree(self, name = "", root_id = -1):
        if name not in self._forest:
            self._forest[name] = root_id
            return True
        else:
            return False

    def __bytes__(self) -> bytes:
        meta = struct.pack('ii', self.METADATA_NODE, len(self._forest))
        for name, rootid in self._forest.items():
            fmt = "ii{}s".format(len(name))
            meta = meta + struct.pack(fmt, rootid, len(name), bytes(name, 'utf-8'))
        return meta

    @classmethod
    def from_bytes(cls, data: bytes) -> MWayTreeNode:
        node = cls()
        _, count = struct.unpack_from('ii', data)
        offset = struct.calcsize('ii')
        for i in range(0, count):
            fmt = 'ii'
            rootid, strlen, = struct.unpack_from(fmt, data, offset)
            offset = offset + struct.calcsize(fmt)
            fmt = "{}s".format(strlen)
            name, = struct.unpack_from(fmt, data, offset)
            offset = offset + struct.calcsize(fmt)
            node.add_tree(name.decode('utf-8'), rootid)
        return node

    def query_value(self, store, timestamp):
        return None

class MWayTreeInternalNode(MWayTreeNode):

    def __init__(self):
        self._children = []
        # TODO: avoid invalid temporary aggregations
        self._aggregation = Aggregation.from_sample((0,0))

    def children(self):
        return self._children

    def add_child(self, key: int, aggregation: Aggregation):
        if len(self._children) == 0:
            self._aggregation = aggregation
        elif aggregation is not None: # filter out temp internal node
            self._aggregation = self._aggregation + aggregation
        self._children.append((key, aggregation))

    @property
    def timespan(self) -> TimeSpan:
        return self._aggregation.timespan

    @property
    def aggregation(self) -> Aggregation:
        return self._aggregation

    def __bytes__(self):
        data = struct.pack("ii", self.INTERNAL_NODE, int(len(self._children)))
        for key, agg in self._children:
            # KEY BEGIN END MIN MAX FIRST LAST MEAN M2
            timespan = agg.timespan if agg is not None else TimeSpan(0,0)
            metrics = agg.metrics if agg is not None else ValueMetrics.from_value(0)
            data = data + struct.pack("iiiidddddd", key, timespan.begin, timespan.end,
                    metrics.count, metrics.minimum, metrics.maximum, metrics.first, metrics.last,
                    metrics.mean, metrics._m2)
        return data

    @classmethod
    def from_bytes(cls, data: bytes) -> MWayTreeNode:
        node = cls()
        fmt = 'ii'
        _, lc = struct.unpack_from(fmt, data)
        offset = struct.calcsize(fmt)
        fmt = "iiiidddddd"
        for _ in range(0, lc):
            fields = tuple(struct.unpack_from(fmt, data, offset))
            key = fields[0]
            timespan = TimeSpan(fields[1], fields[2])
            metrics = ValueMetrics(*fields[3:])
            node.add_child(key, Aggregation(timespan, metrics))
            offset = offset + struct.calcsize(fmt)

        return node

    def select_range(self, selection_span: TimeSpan):
        """ Select a range of nodes falling between `begin` and `end` """
        assert self._children

        in_range_children = []
        full_span = self.timespan
        if selection_span.overlaps(full_span):
            # In range, so:
            # Some overlap!
            # Find first node:
            for node in self._children:
                if selection_span.overlaps(node.timespan):
                    in_range_children.append(node)

        return in_range_children

    def select_all(self):
        return self._children

    def query_value(self, store, timestamp):
        for key, agg in self._children:
            if timestamp >= agg.timespan.begin and timestamp <= agg.timespan.end:
                node = store.get(key)
                if node is not None:
                    return node.query_value(store, timestamp)
                else:
                    return (agg.timespan.begin + (agg.timespan.end - agg.timespan.begin) / 2, mean)
        return None

class MWayTreeLeafNode(MWayTreeNode):

    def __init__(self):
        self._samples = []
        self._count = 0

    def append(self, sample):
        _, y = sample
        self._count = self._count + 1
        if self._count == 1:
            self._min = y
            self._max = y
            self._sum = y
            self._mean = y
            self._m2 = 0
        else:
            self._min = min(self._min, y)
            self._max = max(self._max, y)
            d0 = y - self._mean
            d1 = d0 / self._count
            self._mean = self._mean + d1
            d2 = d0 - d1
            self._m2 = self._m2 + d0 * d2
        self._samples.append(sample)

    def samples(self):
        return self._samples

    @property
    def timespan(self):
        if len(self._samples) > 0:
            begin = self._samples[0][0]
            end = self._samples[-1][0]
            return TimeSpan(begin, end)
        return TimeSpan(0, 0)

    @property
    def metrics(self):
        if self._count == 0:
            return Metrics.from_value(0)
        return ValueMetrics(self._count, self._min, self._max, self._samples[0][1], self._samples[-1][1], self._mean, self._m2)

    @property
    def aggregation(self) -> Aggregation:
        return Aggregation(self.timespan, self.metrics)

    def __bytes__(self):
        ls = len(self.samples())
        fmt = "ii{}d".format(ls * 2)
        _samples = [x for y in self.samples() for x in y]
        return struct.pack(fmt, self.LEAF_NODE, ls, *_samples)

    @classmethod
    def from_bytes(cls, data: bytes) -> MWayTreeNode:
        fmt = 'ii'
        _, ls = struct.unpack_from(fmt, data)
        offset = struct.calcsize(fmt)
        fmt = "{}d".format(ls * 2)
        plain = list(struct.unpack_from(fmt, data, offset))
        samples = list(zip(plain[0::2], plain[1::2]))
        node = cls()
        for sample in samples:
            node.append(sample)
        return node

    def select_range(self, selection_span: TimeSpan):
        """ Select a range of samples falling between `begin` and `end` """
        return [(x,y) for x,y in self._samples if selection_span.contains_timestamp(x)]

    def select_all(self):
        """ Retrieve all samples in this node.
        """
        return self._samples

    def query_value(self, store, timestamp):
        """ Returning None causes upper levels to return a mean value; is that a good choice?
        """
        if not self._samples:
            return None

        index = bisect.bisect_left(self._samples, (timestamp, 0))

        return self._samples[index] if index < len(self._samples) else None


class MWayTree:

    def __init__(self, store: Store, name: str, rootid: int, fan_out: int = 16):
        self._store = store
        self._name = name
        self._rootid = rootid
        self._fan_out = fan_out
        self._depth = 0
        self._ancestors = []
        self._open_tree()
        self._leaf = MWayTreeLeafNode()

    def _open_tree(self):
        self._ancestors = []
        key = self._rootid
        node = self._store.get(key)
        while isinstance(node, MWayTreeInternalNode):
            self._ancestors.append((key,node))
            if len(node.children()) == 0:
                break
            key, _ = node.children()[-1]
            node = self._store.get(key)
            self._depth = self._depth + 1

    def _close_tree(self):
        parent_key, parent = self._get_parent()
        key = self._add_node(self._leaf)
        # add this leaf to its parent with (begin, end, mean)
        parent.add_child(key, self._leaf.aggregation)
        self._store.set(parent_key, parent)
        self._maybe_expand_tree()

    # Adds a new layer of internals starting from the last ancestors, if any
    def _get_parent(self) -> (int, MWayTreeNode):
        return self._ancestors[-1]

    def _maybe_expand_tree(self):
        # Traverse the tree upwards if nodes are completely filled
        parent = self._ancestors[-1][1] if len(self._ancestors) > 0 else None
        while parent is not None and len(parent.children()) == self._fan_out:
            key, node = self._ancestors.pop()
            if len(self._ancestors) > 0:
                parent_key, parent = self._ancestors[-1]
                parent._children[-1] = (key, node.aggregation)
                self._store.set(parent_key, parent)
            else:
                parent = None

        if parent is None:
            # Complete filling, replace old root with new internal node,
            # increasing the capacity of the tree.
            root = self._store.get(self._rootid)
            new_key_for_old_root = self._add_node(root)
            parent = MWayTreeInternalNode()
            parent.add_child(new_key_for_old_root, root.aggregation)
            self._store.set(self._rootid, parent)
            # depth just increased by adding another level
            self._depth = self._depth + 1
            self._ancestors = [(self._rootid, parent)]

        while len(self._ancestors) <= self._depth:
            parent_key, parent = self._ancestors[-1]
            child = MWayTreeInternalNode()
            child_key = self._add_node(child)
            parent.add_child(child_key, None)
            self._store.set(parent_key, parent)
            self._ancestors.append((child_key, child))

    def _add_node(self, node: MWayTreeNode) -> int:
        key = self._store.next_key()
        self._store.set(key, node)
        return key

    def append(self, sample: (int, float)):
        self._leaf.append(sample)
        if len(self._leaf.samples()) == self._fan_out:
            self._close_tree()
            self._leaf = MWayTreeLeafNode()

    @classmethod
    def enhance(nodes, selection_span):
        """ Enhance resolution by descending into child nodes in the selected time span.
        """
        assert nodes
        new_nodes = []
        if len(nodes) == 1:
            new_nodes.extend(nodes[0].select_range(selection_span))
        else:
            # Assume here first and last selected node overlap partially.
            assert len(nodes) > 1
            new_nodes.extend(nodes[0].select_range(selection_span))
            for node in nodes[1:-1]:
                new_nodes.extend(node.select_all())
            new_nodes.extend(nodes[-1].select_range(selection_span))
        return new_nodes

    def query(self, selection_timespan: TimeSpan, min_count):
        """ Query this tree for some data between the given points.
        """
        # Initial query result:
        node = self._store.get(self._rootid)
        selection = node.select_range(selection_timespan)

        # Enhance resolution, while not enough samples.
        while (
            selection
            and len(selection) < min_count
            and isinstance(selection[0], MWayTreeNode)
        ):
            selection = MWayTree.enhance(selection, selection_timespan)

        # TODO: Take metrics from internal nodes:
        if selection and isinstance(selection[0], MWayTreeNode):
            selection = [n.aggregation for n in selection]

        return selection

    def query_metrics(self, selection_timespan: TimeSpan) -> Aggregation:
        """ Retrieve aggregation from a given range. """
        node = self._store.get(self._rootid)

        partially_selected = [node]
        selected_aggregations = []
        selected_samples = []

        while partially_selected:
            partial_node = partially_selected.pop()
            selection = partial_node.select_range(selection_timespan)
            if selection:
                if isinstance(selection[0], MWayTreeNode):
                    for node in selection:
                        aggregation = node.aggregation
                        if selection_timespan.covers(aggregation.timespan):
                            selected_aggregations.append(aggregation)
                        else:
                            partially_selected.append(node)
                else:
                    selected_samples.extend(selection)

        # print(len(selected_aggregations), len(selected_samples))

        if selected_samples:
            selected_aggregations.append(Aggregation.from_samples(selected_samples))

        if selected_aggregations:
            return Aggregation.from_aggregations(selected_aggregations)


    def query_value(self, timestamp):
        """ Query value closest to the given timestamp.

        Return a timestamp value pair as an observation point.
        """
        node = self._store.get(self._rootid)
        return node.query_value(self._store, timestamp) if node is not None else None


class TimeSeriesDatabase:

    def __init__(self, store: Store):
        self._store = store
        self._signals = dict()

        # Read metadata from the store, or create it if non-existent
        self._metadata = store.get(0)
        if self._metadata is None:
            self._metadata = MWayTreeMetadataNode()
            self._store.set(0, self._metadata)

        for name, rootid in self._metadata:
            self._signals[name] = MWayTree(self._store, name, rootid)

    def add(self, name = "") -> MWayTree:
        if name not in self._signals:
            node = MWayTreeInternalNode()
            rootid = self._store.add(node)
            self._metadata.add_tree(name, rootid)
            self._store.set(0, self._metadata)

            signal = MWayTree(self._store, name, rootid)
            self._signals[name] = signal
            return signal
        else:
            raise ValueError

    def get(self, name) -> MWayTree:
        return self._signals[name]

    def count(self) -> int:
        return len(self._signals)

    def __iter__(self):
        for name, signal in self._signals.items():
            yield (name, signal)

    def __repr__(self):
        return f''.join(['{} -> {}\n'.format(name, signal._rootid) for name, signal in self])


class KeyValueStoreTest(unittest.TestCase):

    @unittest.skip
    def test_key_generation(self):
        store = KeyValueStore('test.db')
        for i in range(store._lastkey, store._lastkey + 100):
            self.assertEqual(i + 1, store.next_key())

    @unittest.skip
    def test_simple_tree(self):
        store = KeyValueStore('test.db')
        leaf0 = store.add(MWayTreeLeafNode([(1,1), (2,2), (3,3)]))
        leaf1 = store.add(MWayTreeLeafNode([(4,4), (5,5), (6,6)]))
        node2 = store.add(MWayTreeInternalNode([leaf0, leaf1], [3.5, 1, 6]))
        store.printTree(node2)

    @unittest.skip
    def test_modify_tree(self):
        store = KeyValueStore('test.db')
        leaf0 = store.add(MWayTreeLeafNode([(1,1), (2,2), (3,3)]))
        leaf1 = store.add(MWayTreeLeafNode([(4,4), (5,5), (6,6)]))
        node2 = store.add(MWayTreeInternalNode([leaf0, leaf1], [3.5, 1, 6]))

        leaf3 = store.add(MWayTreeLeafNode([(7,7), (8,8), (9,9)]))
        leaf4 = store.add(MWayTreeLeafNode([(10,10), (11,11), (12,12)]))
        node5 = store.add(MWayTreeInternalNode([leaf3, leaf4], [8.5, 7, 12]))
        node6 = store.add(MWayTreeInternalNode([node2, node5], [6, 1, 12]))

        store.printTree(node6)

    @unittest.skip
    def test_large_database(self):
        store = KeyValueStore('test.db')
        MWayTreeMWayTreeLeafNode = MWayTreeLeafNode([(1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9), (10,10)])
        leaf = store.add(MWayTreeMWayTreeLeafNode)
        leafs = [leaf]
        for i in range(0, 100000):
            leaf = store.add(MWayTreeMWayTreeLeafNode)
            leafs.append(leaf)
        node = store.add(MWayTreeInternalNode(leafs, [5, 1, 10]))
        #store.printTree(node)

    @unittest.skip
    def test_batch_write(self):
        store = KeyValueStore('test.db')
        with store as transation:
            MWayTreeMWayTreeLeafNode = MWayTreeLeafNode([(1,1), (2,2), (3,3), (4,4), (5,5), (6,6), (7,7), (8,8), (9,9), (10,10)])
            leaf = store.add(MWayTreeMWayTreeLeafNode)
            leafs = [leaf]
            for i in range(0, 100000):
                leaf = store.add(MWayTreeMWayTreeLeafNode)
                leafs.append(leaf)
            node = store.add(MWayTreeInternalNode(leafs, [5, 1, 10]))
        #store.printTree(node)


class TSDBTest(unittest.TestCase):

    @unittest.skip
    def test_add_signal_kvstore(self):
        store = KeyValueStore('test.db')
        tsdb = TimeSeriesDatabase(store)
        s = tsdb.add("TestSignal{}".format(len(tsdb.signals())))
        tsdb.printSignals()

    @unittest.skip
    def test_add_signal_dictionary(self):
        store = Dictionary()
        tsdb = TimeSeriesDatabase(store)
        s = tsdb.add("TestSignal")
        for i in range(0, 1000000):
            s.append((i,i))
        #tsdb.printSignals()

    @unittest.skip
    def test_add_samples(self):
        store = KeyValueStore('test.db')
        tsdb = TimeSeriesDatabase(store)
        s = tsdb.add("TestSignalWithData{}".format(tsdb.count()))
        for i in range(0, 1000000):
            s.append((i,i))
        #store.printTree(s._rootid)
        #store.printDot(s._name, s._rootid)

    @unittest.skip
    def test_open_store(self):
        store = KeyValueStore('test.db')
        tsdb = TimeSeriesDatabase(store)
        print(tsdb)

    @unittest.skip
    def test_print_tree(self):
        store = KeyValueStore('test.db')
        tsdb = TimeSeriesDatabase(store)
        for name, signal in tsdb.signals().items():
            #print('Signal {} [{}]'.format(name, signal._rootid))
            store.printTree(signal._rootid)
            #store.printDot(name, signal._rootid)

    #@unittest.skip
    def test_query_value(self):
        store = KeyValueStore('test.db')
        tsdb = TimeSeriesDatabase(store)
        s = tsdb.add("TestSignalWithData{}".format(tsdb.count()))

        for i in range(0, 1024):
            s.append((i,i))

        s._close_tree()
        store.printTree(s._rootid)

        self.assertEqual(23, s.query_value(23)[1])

if __name__ == "__main__":
    unittest.main()
