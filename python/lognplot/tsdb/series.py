import abc
import bisect
from .metrics import Metrics
from .aggregation import Aggregation
from ..time import TimeSpan
from typing import Iterator, Tuple

class Series:

    @classmethod
    def create(cls, db, name: str, identifier: int):
        """ Creates a new time-series. """
        return cls(db, name, identifier)

    @property
    def name(self) -> str:
        """ Returns the name of the time-series. """
        raise NotImplementedError()

    @property
    def identifier(self) -> int:
        """ Returns the identifier of the time-series. """
        raise NotImplementedError()

    def __len__(self) -> int:
        """ Returns the number of samples within the time-series. """
        raise NotImplementedError()

    def __iter__(self) -> Iterator[Tuple[float, float]]:
        """ Iterate over the samples within the time-series. """
        raise NotImplementedError()

    def add(self, sample: Tuple[float, float]):
        """ Appends a single sample to the time-series. """
        raise NotImplementedError()

    def clear(self):
        """ Removes all samples from the time-series. """
        raise NotImplementedError()

    def query(self, selection_timespan: TimeSpan, min_count: int):
        """ Query the time-series for data within the specified timespan.
            It either returns a list of aggregations, or a list of samples. """
        raise NotImplementedError()

    def query_metrics(self, selection_timespan: TimeSpan) -> Aggregation:
        """ Retrieve metrics on the data within the specified timespan. """
        raise NotImplementedError()

    def query_value(self, timestamp) -> Tuple[float, float]:
        """ Query the time-series for the value closest to the given timestamp.
            Return a timestamp value pair as an observation point. """
        raise NotImplementedError()

    def get_type(self):
        return "signal"

        # TODO: support this
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


