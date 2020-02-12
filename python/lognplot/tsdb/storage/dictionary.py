from typing import Iterator
from ..db import TimeSeriesDatabase
from ..series import Series

class Dictionary(TimeSeriesDatabase):

    def __init__(self, cls: Series):
        self._cls = cls
        self._series = dict()
        self._data = dict()
        self._lastkey = 0

    def __iter__(self) -> Iterator[Series]:
        for key, name in self._series.items():
            yield self._cls.create(self, name, key)

    def create(self, name: str) -> Series:
        self._series[key] = name
        return self._cls.create(self, name, key)

    def delete(self, series: Series):
        # Remove the series from the index.
        key = series.identifier()
        self.clear(key)
        if key in self._series:
            del self._series[key]
        # Clear all the samples out of the database.
        series.clear()

    def next_key(self):
        self._lastkey = self._lastkey + 1
        return self._lastkey

    def get(self, key: int) -> bytes:
        if key in self._data:
            return self._data[key]
        return None

    def set(self, key: int, data: bytes):
        self._data[key] = data

    def add(self, data: bytes) -> int:
        key = self.next_key()
        self._data[key] = data
        return key

    def clear(self, key: int):
        if key in self._data:
            del self._data[key]


