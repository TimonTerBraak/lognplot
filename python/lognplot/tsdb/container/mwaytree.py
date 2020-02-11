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
    def from_bytes(cls, data: bytes):
        raise NotImplementedError()

    @classmethod
    def get(cls, db: TimeSeriesDatabase, key: int):
        data = db.get(key)
        if data is None or len(data) == 0:
            return None
        count, = struct.unpack_from('i', data)
        if count <= 0:
            return MWayTreeInternalNode.from_bytes(data)
        else:
            return MWayTreeLeafNode.from_bytes(data)

    @abc.abstractmethod
    def select_range(self, selection_span: TimeSpan):
        raise NotImplementedError()

    @abc.abstractmethod
    def select_all(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def query_value(self, db, timestamp):
        raise NotImplementedError()

#class MWayTreeMetadataNode(MWayTreeNode):
#
#    """
#        Forest is a dictionary with name -> root_id
#    """
#    def __init__(self):
#        self._forest = dict()
#        # TODO: add absolute time for t0, and timebase
#
#    def __iter__(self):
#        for name, root_id in self._forest.items():
#            yield (name, root_id)
#
#    def add_tree(self, name = "", root_id = -1):
#        if name not in self._forest:
#            self._forest[name] = root_id
#            return True
#        else:
#            return False
#
#    def __bytes__(self) -> bytes:
#        meta = struct.pack('ii', self.METADATA_NODE, len(self._forest))
#        for name, rootid in self._forest.items():
#            fmt = "ii{}s".format(len(name))
#            meta = meta + struct.pack(fmt, rootid, len(name), bytes(name, 'utf-8'))
#        return meta
#
#    @classmethod
#    def from_bytes(cls, data: bytes) -> MWayTreeNode:
#        node = cls()
#        _, count = struct.unpack_from('ii', data)
#        offset = struct.calcsize('ii')
#        for i in range(0, count):
#            fmt = 'ii'
#            rootid, strlen, = struct.unpack_from(fmt, data, offset)
#            offset = offset + struct.calcsize(fmt)
#            fmt = "{}s".format(strlen)
#            name, = struct.unpack_from(fmt, data, offset)
#            offset = offset + struct.calcsize(fmt)
#            node.add_tree(name.decode('utf-8'), rootid)
#        return node
#
#    def query_value(self, db, timestamp):
#        return None

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
        # count of child nodes is negative
        data = struct.pack("i", int(-len(self._children)))
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
        lc, = struct.unpack_from('i', data)
        offset = struct.calcsize('i')
        fmt = "iiiidddddd"
        for _ in range(0, -lc): # count of child nodes is negative
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

    def query_value(self, db, timestamp):
        for key, agg in self._children:
            if timestamp >= agg.timespan.begin and timestamp <= agg.timespan.end:
                node = self.get(self._db, key)
                if node is not None:
                    return node.query_value(db, timestamp)
                else:
                    return (agg.timespan.begin + (agg.timespan.end - agg.timespan.begin) / 2, mean)
        return None

class MWayTreeLeafNode(MWayTreeNode):

    def __init__(self):
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

    @property
    def aggregation(self) -> Aggregation:
        return Aggregation(self.timespan, self.metrics)

    def __len__(self):
        return len(self._samples)

    def __bytes__(self):
        ls = len(self.samples())
        fmt = "i{}d".format(ls * 2)
        _samples = [x for y in self.samples() for x in y]
        return struct.pack(fmt, ls, *_samples)

    @classmethod
    def from_bytes(cls, data: bytes) -> MWayTreeNode:
        ls, = struct.unpack_from('i', data)
        offset = struct.calcsize('i')
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

    def query_value(self, db, timestamp):
        """ Returning None causes upper levels to return a mean value; is that a good choice?
        """
        if not self._samples:
            return None

        index = bisect.bisect_left(self._samples, (timestamp, 0))

        return self._samples[index] if index < len(self._samples) else None


class MWayTree(Series):

    def __init__(self, db: TimeSeriesDatabase, name: str, rootid: int = -1, fan_out: int = 16):
        self._db = db
        self._name = name
        self._rootid = rootid
        self._fan_out = fan_out
        self._depth = 0
        self._ancestors = []
        self._leaf = MWayTreeLeafNode()

        if rootid > 0:
            self._open_tree()
        else:
            self._maybe_expand_tree()
            self._db.add_to_index(self._rootid, name)

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
            parent = MWayTreeInternalNode()
            # Complete filling, replace old root with new internal node,
            # increasing the capacity of the tree.
            root = MWayTreeNode.get(self._db, self._rootid) if self._rootid > 0 else None
            if root is None:
                # Tree has not been dbd yet
                self._rootid = self._db.next_key()
            else:
                # Move the entire tree as a subtree under a newly created root node.
                new_key_for_old_root = self._add_node(root)
                parent.add_child(new_key_for_old_root, root.aggregation)
            self._db.set(self._rootid, bytes(parent))
            # depth just increased by adding another level
            self._depth = self._depth + 1
            self._ancestors = [(self._rootid, parent)]

        while len(self._ancestors) <= self._depth:
            parent_key, parent = self._ancestors[-1]
            child = MWayTreeInternalNode()
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
            self._leaf = MWayTreeLeafNode()

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

    def query(self, selection_timespan: TimeSpan, min_count):
        """ Query this tree for some data between the given points.
        """
        # Initial query result:
        node = MWayTreeNode.get(self._db, self._rootid)
        selection = node.select_range(selection_timespan)

        # Enhance resolution, while not enough samples.
        while (
            selection
            and len(selection) < min_count
            and isinstance(selection[0], MWayTreeNode)
        ):
            selection = self._enhance(selection, selection_timespan)

        # TODO: Take metrics from internal nodes:
        if selection and isinstance(selection[0], MWayTreeNode):
            selection = [n.aggregation for n in selection]

        return selection

    def query_metrics(self, selection_timespan: TimeSpan) -> Aggregation:
        """ Retrieve aggregation from a given range. """
        node = MWayTreeNode.get(self._db, self._rootid)

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

        if selected_samples:
            selected_aggregations.append(Aggregation.from_samples(selected_samples))

        if selected_aggregations:
            return Aggregation.from_aggregations(selected_aggregations)


    def query_value(self, timestamp):
        """ Query value closest to the given timestamp.

        Return a timestamp value pair as an observation point.
        """
        node = MWayTreeNode.get(self._db, self._rootid)
        return node.query_value(self._db, timestamp) if node is not None else None

