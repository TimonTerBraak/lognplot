import abc
import bisect
import struct
from typing import Sequence
from ..aggregation import Aggregation
from ..metrics import Metrics, ValueMetrics
from ..db import TimeSeriesDatabase
from ..series import Series
from ...time import TimeSpan

class MWayTreeNode:

    @abc.abstractmethod
    def __bytes__(self) -> bytes:
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def from_bytes(cls, db: TimeSeriesDatabase, data: bytes):
        raise NotImplementedError()

    @classmethod
    def get(cls, db: TimeSeriesDatabase, key: int):
        data = db.get(key)
        if data is None or len(data) == 0:
            return None
        count, = struct.unpack_from('i', data)
        if count <= 0:
            return MWayTreeInternalNode.from_bytes(db, data)
        else:
            return MWayTreeLeafNode.from_bytes(db, data)

    @abc.abstractmethod
    def select_range(self, selection_span: TimeSpan):
        raise NotImplementedError()

    @abc.abstractmethod
    def select_all(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def query_value(self, timestamp):
        raise NotImplementedError()


class MWayTreeInternalNode(MWayTreeNode):

    def __init__(self, db):
        self._db = db
        self._children = []

    def children(self):
        return self._children

    def add_child(self, key: int, aggregation: Aggregation):
        self._children.append((key, aggregation))

    @property
    def timespan(self) -> TimeSpan:
        return self._aggregation.timespan

    @property
    def aggregation(self) -> Aggregation:
        if len(self._children) > 0:
            # NOTE: the pre-calculated aggregation may already be available one level up!
            return Aggregation.from_aggregations([agg for _, agg in self.children()])

    def __bytes__(self):
        # count of child nodes is negative
        data = struct.pack("i", int(-len(self._children)))
        for key, agg in self._children:
            # KEY BEGIN END MIN MAX FIRST LAST MEAN M2
            timespan = agg.timespan if agg is not None else TimeSpan(0,0)
            metrics = agg.metrics if agg is not None else ValueMetrics(0, 1e9, -1e9, 0, 0, 0, 0)
            data = data + struct.pack("iddidddddd", key, timespan.begin, timespan.end,
                    metrics.count, metrics.minimum, metrics.maximum, metrics.first, metrics.last,
                    metrics.mean, metrics._m2)
        return data

    @classmethod
    def from_bytes(cls, db: TimeSeriesDatabase, data: bytes) -> MWayTreeNode:
        node = cls(db)
        lc, = struct.unpack_from('i', data)
        offset = struct.calcsize('i')
        fmt = "iddidddddd"
        for _ in range(0, -lc): # count of child nodes is negative
            fields = tuple(struct.unpack_from(fmt, data, offset))
            key = fields[0]
            timespan = TimeSpan(fields[1], fields[2])
            metrics = ValueMetrics(*fields[3:])
            node.add_child(key, Aggregation(timespan, metrics))
            offset = offset + struct.calcsize(fmt)

        return node

    def query_value(self, timestamp):
        sample = None
        children = [key for key, agg in self._children if timestamp >= agg.timespan.begin and timestamp <= agg.timespan.end]
        if len(children) > 0:
            # It should be just a single node, right?
            node = self.get(self._db, children[0])
            sample = node.query_value(timestamp)

        return sample


class MWayTreeLeafNode(MWayTreeNode):

    def __init__(self, db: TimeSeriesDatabase):
        self._db = db
        self._samples = []
        self._mean = 0
        self._m2 = 0

    def add(self, sample):
        _, y = sample
        if len(self._samples) == 0:
            self._mean = y
            self._m2 = 0
        else:
            delta = y - self._mean
            self._mean = self._mean + (delta / len(self._samples))
            delta2 = y - self._mean
            self._m2 = self._m2 + delta * delta2
        self._samples.append(sample)

    @property
    def timespan(self):
        x0, x1 = 0, 0
        if len(self._samples) > 0:
            x0, _ = self._samples[0]
            x1, _ = self._samples[-1]
        return TimeSpan(x0, x1)

    @property
    def metrics(self):
        if len(self._samples) > 0:
            values = [y for _, y in self._samples]
            return ValueMetrics(len(self._samples), min(values), max(values), values[0], values[-1], self._mean, self._m2)
        else:
            # NOTE: we also need metrics for empty nodes to be able to know that it holds 0 samples.
            # NOTE: merging of metrics now considers the '0 samples' case.
            return ValueMetrics(0, 0, 0, 0, 0, 0, 0)

    def samples(self):
        return self._samples

    @property
    def aggregation(self) -> Aggregation:
        return Aggregation(self.timespan, self.metrics)

    def __len__(self):
        return len(self._samples)

    def __bytes__(self):
        ls = len(self._samples)
        fmt = "i{}d".format(ls * 2)
        _samples = [x for y in self._samples for x in y]
        return struct.pack(fmt, ls, *_samples)

    @classmethod
    def from_bytes(cls, db: TimeSeriesDatabase, data: bytes) -> MWayTreeNode:
        ls, = struct.unpack_from('i', data)
        fmt = "i{}d".format(ls * 2)
        plain = struct.unpack(fmt, data)
        samples = list(zip(plain[1::2], plain[2::2]))
        node = cls(db)
        for sample in samples:
            node.add(sample)
        return node

    def query_value(self, timestamp):
        if not self._samples:
            return None

        index = bisect.bisect_left(self._samples, (timestamp, 0))

        if index == len(self._samples):
            return self._samples[-1]
        elif index == 0:
            return self._samples[0]
        elif (timestamp - self._samples[index - 1][0]) < (timestamp - self._samples[index][0]):
            # 'previous' sample is closer to the requested timestamp
            return self._samples[index - 1]
        else:
            return self._samples[index]


class MWayTree(Series):

    def __init__(self, db: TimeSeriesDatabase, name: str, rootid: int = -1, fan_out: int = 64):
        self._db = db
        self._name = name
        self._rootid = rootid
        self._fan_out = fan_out
        self._depth = 0
        self._ancestors = []
        self._leaf = None
        self._open_tree()

    def name(self) -> str:
        return self._name

    def identifier(self) -> int:
        return self._rootid

    def __del__(self):
        if self._leaf is not None:
            self._add_leaf(self._leaf)

    def _open_tree(self):
        key = self._rootid
        node = MWayTreeNode.get(self._db, key)
        self._ancestors = []
        while isinstance(node, MWayTreeInternalNode):
            self._ancestors.append((key,node))
            if len(node.children()) > 0:
                key, _ = node.children()[-1]
                node = MWayTreeNode.get(self._db, key)
            else:
                break
        self._depth = len(self._ancestors)

    def _get_parent_with_capacity(self) -> (int, MWayTreeNode):
        if len(self._ancestors) == 0:
            # A new layer to the tree.
            self._add_branch()
        elif len(self._ancestors) < self._depth:
            self._grow_branch()
        key, parent = self._ancestors[-1]
        if len(parent.children()) == self._fan_out:
            # Parent is already completely filled.
            self._ancestors.pop()
            key, parent = self._get_parent_with_capacity()

        return (key, parent)

    def _add_leaf(self, node: MWayTreeLeafNode):
        _, parent = self._get_parent_with_capacity()
        key = self._add_node(node)
        # add this leaf to its parent with its aggregation calculated in the loop.
        parent.add_child(key, None)

        # Traverse the tree upwards (reverse) to update all aggregations
        for pkey, pnode in self._ancestors[::-1]:
            pnode._children[-1] = (key, node.aggregation)
            self._db.set(pkey, bytes(pnode))

            if len(pnode.children()) == self._fan_out:
                # remove parent from the list of ancestors (with capacity)
                self._ancestors.pop()

            if len(self._ancestors) == 0:
                # Have to insert new root node
                self._add_branch()
                break

            key, node = pkey, pnode

    def _add_branch(self):
        parent = MWayTreeInternalNode(self._db)
        # Complete filling, replace old root with new internal node,
        # increasing the capacity of the tree.
        root = MWayTreeNode.get(self._db, self._rootid) if self._rootid > 0 else None
        if root is not None:
            # Move the entire tree as a subtree under a newly created root node.
            new_key_for_old_root = self._add_node(root)
            parent.add_child(new_key_for_old_root, root.aggregation)
        self._db.set(self._rootid, bytes(parent))
        # depth just increased by adding another level
        self._depth = self._depth + 1
        self._ancestors = [(self._rootid, parent)]
        self._grow_branch()

    def _grow_branch(self):
        while len(self._ancestors) < self._depth:
            parent_key, parent = self._ancestors[-1]
            child = MWayTreeInternalNode(self._db)
            child_key = self._add_node(child)
            parent.add_child(child_key, None)
            self._db.set(parent_key, bytes(parent))
            self._ancestors.append((child_key, child))

    def _add_node(self, node: MWayTreeNode) -> int:
        key = self._db.next_key()
        self._db.set(key, bytes(node))
        return key

    def add(self, sample: (int, float)):
        if self._leaf is None:
            self._leaf = MWayTreeLeafNode(self._db)
        self._leaf.add(sample)
        if len(self._leaf) == self._fan_out:
            self._add_leaf(self._leaf)
            self._leaf = None

    def __len__(self):
        agg = self.query_metrics(None)
        return agg.metrics.count if agg is not None else 0

    def __iter__(self):
        key = self._rootid
        node = MWayTreeNode.get(self._db, key)
        nodes = [(key,node)]
        while len(nodes) > 0:
            keynode, nodes = nodes[0], nodes[1:]
            key, node = keynode
            if isinstance(node, MWayTreeInternalNode):
                for child, aggregation in node.children():
                    nodes.append((child, MWayTreeNode.get(self._db, child)))
            else:
                for sample in node.samples():
                    yield sample

    def clear(self):
        key = self._rootid
        node = MWayTreeNode.get(self._db, key)
        nodes = [(key,node)]
        while len(nodes) > 0:
            keynode, nodes = nodes[0], nodes[1:]
            key, node = keynode
            if isinstance(node, MWayTreeInternalNode):
                for child, aggregation in node.children():
                    nodes.append((child, MWayTreeNode.get(self._db, child)))
            self._db.clear(key)

    def query(self, selection_timespan: TimeSpan, min_count):
        """ Query this tree for some data between the given points.
        """
        node = MWayTreeNode.get(self._db, self._rootid)
        nodes = [node]
        selection = []

        while len(selection) < min_count and len(nodes) > 0:
            internals = [node for node in nodes if isinstance(node, MWayTreeInternalNode)]
            children = [n for node in internals for n in node.children()]
            if len(children) == 0:
                leafs = [node for node in nodes if isinstance(node, MWayTreeLeafNode)]
                return [sample for leaf in leafs for sample in leaf.samples() if selection_timespan.begin <= sample[0] <= selection_timespan.end]
            else:
                selection = [(key, agg) for key, agg in children if selection_timespan is None or selection_timespan.overlaps(agg.timespan)]
                nodes = [MWayTreeNode.get(self._db, key) for key, _ in selection]

        return [agg for _, agg in selection]

    def query_metrics(self, selection_timespan: TimeSpan) -> Aggregation:
        """ Retrieve aggregation from a given range. """
        root = MWayTreeNode.get(self._db, self._rootid)

        selection = []

        nodes = [root]
        while len(nodes) > 0:
            internals = [node for node in nodes if isinstance(node, MWayTreeInternalNode)]
            children = [n for node in internals for n in node.children()]
            selection.extend([agg for _, agg in children if selection_timespan is None or selection_timespan.covers(agg.timespan)])
            nodes = [MWayTreeNode.get(self._db, key) for key, agg in children
                            if selection_timespan is not None
                                and not selection_timespan.covers(agg.timespan)
                                    and selection_timespan.overlaps(agg.timespan)]

        if self._leaf is not None and (selection_timespan is not None or selection_timespan.covers(self._leaf.aggregation.timespan)):
            selection.extend(self._leaf.aggregation)

        if len(selection) > 0:
            return Aggregation.from_aggregations(selection)

    def query_value(self, timestamp):
        """ Query value closest to the given timestamp.

        Return a timestamp value pair as an observation point.
        """
        return MWayTreeNode.get(self._db, self._rootid).query_value(timestamp)

    def print_tree(self):
        key = self._rootid
        node = MWayTreeNode.get(self._db, key)
        nodes = [(key,node)]
        while len(nodes) > 0:
            keynode, nodes = nodes[0], nodes[1:]
            key, node = keynode
            if isinstance(node, MWayTreeInternalNode):
                for child, aggregation in node.children():
                    print(f"[{key}] <- {child}<node>: {aggregation}")
                    nodes.append((child, MWayTreeNode.get(self._db, child)))
            else:
                print(f"[{key}]: {node._samples}")

    def print_graphviz(self):
        key = self._rootid
        node = MWayTreeNode.get(self._db, key)
        nodes = [(key,node)]
        print("digraph {")
        while len(nodes) > 0:
            keynode, nodes = nodes[0], nodes[1:]
            key, node = keynode
            if isinstance(node, MWayTreeInternalNode):
                for child, aggregation in node.children():
                    print(f"{key} -> {child};")
                    nodes.append((child, MWayTreeNode.get(self._db, child)))
            else:
                idx = 0
                for x, y in node.samples():
                    print(f"{key} -> {x}.{y}")
                    idx = idx + 1
        print("}")

