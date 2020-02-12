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
        # count of child nodes is negative
        data = struct.pack("i", int(-len(self._children)))
        for key, agg in self._children:
            # KEY BEGIN END MIN MAX FIRST LAST MEAN M2
            timespan = agg.timespan if agg is not None else TimeSpan(0,0)
            metrics = agg.metrics if agg is not None else ValueMetrics.from_value(0)
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
        for key, agg in self._children:
            if timestamp >= agg.timespan.begin and timestamp <= agg.timespan.end:
                node = self.get(self._db, key)
                sample = node.query_value(timestamp)
                break

        return sample

class MWayTreeLeafNode(MWayTreeNode):

    def __init__(self, db: TimeSeriesDatabase):
        self._db = db
        self._samples = []
        self._count = 0

    def add(self, sample):
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

    def __init__(self, db: TimeSeriesDatabase, name: str, rootid: int = -1, fan_out: int = 16):
        self._db = db
        self._name = name
        self._rootid = rootid
        self._fan_out = fan_out
        self._depth = 0
        self._ancestors = []
        self._leaf = MWayTreeLeafNode(self._db)
        self._open_tree()

    def name(self) -> str:
        return self._name

    def identifier(self) -> int:
        return self._rootid

    def _open_tree(self):
        self._ancestors = []
        key = self._rootid
        node = MWayTreeNode.get(self._db, key)
        while isinstance(node, MWayTreeInternalNode):
            self._ancestors.append((key,node))
            if len(node.children()) == 0:
                break
            key, _ = node.children()[-1]
            node = MWayTreeNode.get(self._db, key)
            self._depth = self._depth + 1

        if len(self._ancestors) == 0:
            self._maybe_expand_tree()

    def _close_tree(self):
        parent_key, parent = self._get_parent()
        key = self._add_node(self._leaf)
        # add this leaf to its parent with (begin, end, mean)
        parent.add_child(key, self._leaf.aggregation)
        self._db.set(parent_key, bytes(parent))
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
                self._db.set(parent_key, bytes(parent))
            else:
                parent = None

        if parent is None:
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

        while len(self._ancestors) <= self._depth:
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
        self._leaf.add(sample)
        if len(self._leaf) == self._fan_out:
            self._close_tree()
            self._leaf = MWayTreeLeafNode(self._db)

    def _enhance(self, nodes: Sequence[MWayTreeNode], selection_span: TimeSpan):
        """ Enhance resolution of samples in the selected time span.
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

    def __len__(self):
        node = MWayTreeNode.get(self._db, self._rootid)
        if node:
            return node.aggregation.metrics.count
        return 0

    def query(self, selection_timespan: TimeSpan, min_count):
        """ Query this tree for some data between the given points.
        """
        start, end = selection_timespan.begin, selection_timespan.end
        node = MWayTreeNode.get(self._db, self._rootid)

        nodes = [node]
        selection = [node.aggregation]

        while len(selection) < min_count:
            new_nodes = []
            for node in nodes:
                selection = []
                if isinstance(node, MWayTreeInternalNode):
                    for (key, aggregation) in node.children():
                        if start < aggregation.timespan.end and end > aggregation.timespan.begin:
                            selection.append(aggregation)
                            new_nodes.append(MWayTreeNode.get(self._db, key))
            if len(new_nodes) > 0:
                nodes = new_nodes
            else:
                break

        if len(selection) < min_count:
            selection = [sample for node in nodes for sample in node.samples() if sample[0] >= start and sample[0] <= end]

        return selection

    def query_metrics(self, selection_timespan: TimeSpan) -> Aggregation:
        """ Retrieve aggregation from a given range. """
        node = MWayTreeNode.get(self._db, self._rootid)

        if selection_timespan is None:
            return node.aggregation

        selection = [node.aggregation]
        start, end = selection_timespan.begin, selection_timespan.end
        nodes = [node]
        trimmed = True

        while len(nodes) > 0 and trimmed:
            trimmed = False
            new_nodes = []
            for node in nodes:
                new_selection = []
                if isinstance(node, MWayTreeInternalNode):
                    for (key, aggregation) in node.children():
                        if start < aggregation.timespan.end and end > aggregation.timespan.begin:
                            selection.append(aggregation)
                            new_nodes.append(MWayTreeNode.get(self._db, key))
                        else:
                            trimmed = True
            if trimmed:
                selection = new_selection
                nodes = new_nodes

        if len(selection) > 0:
            return Aggregation.from_aggregations(selection)

    def query_value(self, timestamp):
        """ Query value closest to the given timestamp.

        Return a timestamp value pair as an observation point.
        """
        node = MWayTreeNode.get(self._db, self._rootid)
        return node.query_value(timestamp) if node is not None else None

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

