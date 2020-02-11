import abc
import bisect
from .metrics import Metrics
from .aggregation import Aggregation
from ..time import TimeSpan
from typing import Tuple

class Series:

    @classmethod
    def create(cls, db, name: str, identifier: int):
        return cls(db, name, identifier)

    @property
    def name(self) -> str:
        raise NotImplementedError()

    @property
    def identifier(self) -> int:
        raise NotImplementedError()

    def __len__(self):
        # Return total number of samples!
        raise NotImplementedError()

    def __iter__(self):
        raise NotImplementedError()

    def add(self, sample):
        """ Appends a single sample to the series. """
        raise NotImplementedError()

    def query(self, selection_timespan: TimeSpan, min_count):
        """ Query this tree for some data between the given points.
        """
        raise NotImplementedError()

    def query_metrics(self, selection_timespan: TimeSpan) -> Aggregation:
        """ Retrieve aggregation from a given range. """
        raise NotImplementedError()

    def query_value(self, timestamp):
        """ Query value closest to the given timestamp.

        Return a timestamp value pair as an observation point.
        """
        raise NotImplementedError()























class ZoomSerie(Series):

    def __repr__(self):
        return "ZoomSeries"

    def get_type(self):
        first = next(iter(self), None)
        if first is None:
            return "?"
        else:
            value = first[1]
            if isinstance(value, float):
                return "signal"
            elif isinstance(value, LogRecord):
                return "logger"
            elif isinstance(value, dict):
                return "event"
            else:
                raise NotImplementedError(f"Unknown signal type: {type(value)}")


