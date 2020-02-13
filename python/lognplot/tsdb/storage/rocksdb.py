import rocksdb
import struct
from typing import Iterator
from ..db import TimeSeriesDatabase
from ..series import Series

"""
    NODE: key -> (count, children[], aggregation[])
    LEAF: key -> (count, samples[])
"""
class RocksDB(TimeSeriesDatabase):

    """
        Key 0 is reserved for metadata.
    """
    def __init__(self, cls: Series, filename: str = 'lognplot.db'):
        self._cls = cls
        self._filename = filename
        self._options = rocksdb.Options(create_if_missing=True, compression=rocksdb.CompressionType.lz4_compression)
        self._db = rocksdb.DB(self._filename, self._options)
        self._lastkey = 0

        metadata = self._db.get_live_files_metadata()
        if metadata:
            self._lastkey, = struct.unpack('i', metadata[0]['largestkey'])

    #[name,key,previous]
    def __iter__(self) -> Iterator[Series]:
        # last will always be empty
        cursor, last = struct.unpack_from('ii', self.get(0))
        # Format: [key, next, strlen, name]
        while cursor != last:
            record = self.get(cursor)
            if record is not None:
                key, cursor, length = struct.unpack_from('iii', record, 0)
                name, = struct.unpack_from('{}s'.format(length), record, struct.calcsize('iii'))
                yield self._cls.create(self, name.decode('utf-8'), key)
            else:
                break #raise ValueError

    # TODO remove from index, and delete
    def add_to_index(self, key, name):
        """ Updates the index by appending the (key, name) pair to the linked list
            and updated the list head accordingly.
        """
        metadata = self.get(0)
        if metadata is not None:
            first, last = struct.unpack_from('ii', metadata)
        else:
            first = self.next_key()
            last = first
        reserved = self.add(bytes())
        fmt = 'iii{}s'.format(len(name))
        data = struct.pack(fmt, key, reserved, len(name), bytes(name, 'utf-8'))
        header = struct.pack('ii', first, reserved)
        self.set(last, data)
        self.set(0, header)

    def next_key(self) -> int:
        key = self._lastkey + 1
        self._lastkey = key
        return key

    def set(self, key: int, data: bytes):
        self._db.put(struct.pack('i', key), data)

    def add(self, data: bytes) -> int:
        key = self.next_key()
        self.set(key, data)
        return key

    def get(self, key: int) -> bytes:
        return self._db.get(struct.pack('i', int(key)))

