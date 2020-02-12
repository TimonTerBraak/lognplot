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

    def __len__(self) -> int:
        count = 0
        metadata = self.get(0)
        if metadata:
            count, _, _ = struct.unpack('iii', metadata)
        return count

    def __iter__(self) -> Iterator[Series]:
        # last will always be empty
        metadata = self.get(0)
        if metadata:
            _, cursor, last = struct.unpack_from('iii', metadata)
            while cursor != last:
                record = self.get(cursor)
                if record is not None:
                    cursor, key, length = struct.unpack_from('iii', record, 0)
                    _, _, _, name, = struct.unpack('iii{}s'.format(length), record)
                    series = self._cls.create(self, name.decode('utf-8'), key)
                    yield series
                else:
                    raise ValueError

    def _update_header(self, count: int, first: int, last: int):
        header = struct.pack('iii', count, first, last)
        self.set(0, header)

    def _write_index_entry(self, cursor: int, next_entry: int, key: int, name: str):
        fmt = 'iii{}s'.format(len(name))
        data = struct.pack(fmt, next_entry, key, len(name), bytes(name, 'utf-8'))
        self.set(cursor, data)

    def _remove_from_index(self, key: int):
        metadata = self.get(0)
        if metadata:
            count, first, last = struct.unpack('iii', metadata)
            # Format: [key, next, strlen, name]
            while cursor != last:
                record = self.get(cursor)
                if record is not None:
                    previous = cursor
                    cursor, _key, length = struct.unpack_from('iii', record, 0)
                    if key == _key:
                        # found it!
                        # update previous record to point its next to cursor
                        record = self.get(previous)
                        _, _key, length = struct.unpack_from('iii', record, 0)
                        _, _, _, name, = struct.unpack('iii{}s'.format(length), record)

                        self._write_index_entry(previous, cursor, _key, name)

                        if cursor == last:
                            # update header
                            self._update_header(count - 1, first, previous)
                else:
                    raise ValueError

    def create(self, name: str) -> Series:
        """ Updates the index by appending the (key, name) pair to the linked list
            and updated the list head accordingly.
        """
        metadata = self.get(0)
        key = self.next_key()
        if metadata is not None:
            count, first, last = struct.unpack('iii', metadata)
        else:
            count = 0
            first = self.next_key()
            last = first
        reserved = self.add(bytes())
        self._write_index_entry(last, reserved, key, name)
        self._update_header(count + 1, first, reserved)
        return self._cls.create(self, name, key)

    def delete(self, series: Series):
        key = series.identifier()
        # Remove the series from the index.
        self._remove_from_index(key)
        # Clear all the samples out of the database.
        series.clear()

    def next_key(self) -> int:
        """ For now, the database size is limited by 32-bit (signed) keys in Python,
            combined with the number of samples stored per key. We can strecht this
            limit by reusing keys (overflow) and asking the database to verify that
            the key does not exist (key_may_exist). A future-proof solution is to
            increase the key size used in the Python implementation, as the database
            can handle up-to 8MB key sizes. In the Python implementation, mainly the
            (de-)serialization has to be updated. """
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

    def clear(self, key: int):
        self._db.delete(key)
