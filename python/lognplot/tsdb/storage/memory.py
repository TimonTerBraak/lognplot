from typing import Iterator
from ..db import TimeSeriesDatabase
from ..series import Series

class Memory(TimeSeriesDatabase):

    def __init__(self, cls: Series):
        self._cls = cls
        self._series = dict()
        self._lastkey = 0

    def __len__(self) -> int:
        return len(self._series)

    def __iter__(self) -> Iterator[Series]:
        for key, series in self._series.items():
            yield series

    def create(self, name: str) -> Series:
        key = self.next_key()
        series = self._cls.create(self, name, key)
        self._series[key] = series
        return series

    def delete(self, series: Series):
        # Remove the series from the index.
        key = series.identifier()
        del self._series[key]
        # Clear all the samples out of the database.
        series.clear()

    def next_key(self):
        self._lastkey = self._lastkey + 1
        return self._lastkey

    def get(self, key: int) -> bytes:
        return None

    def set(self, key: int, data: bytes):
        pass

    def add(self, data: bytes) -> int:
        return -1

    def clear(self, key: int):
        pass

