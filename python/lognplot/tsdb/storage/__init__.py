""" Key-Value storage of the time series database.

This module can be used to store time series samples
in a way that they can be queried easily.

"""

from .dictionary import Dictionary
# TODO: optionally import, only when rocksdb is available?
from .rocksdb import RocksDB
