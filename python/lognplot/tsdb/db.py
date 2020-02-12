""" Time series database.
"""

import abc
from typing import Iterator
from .aggregation import Aggregation
from .series import Series
from ..time import TimeSpan

class TimeSeriesDatabase(metaclass=abc.ABCMeta):

    _tokens = 0

    @abc.abstractmethod
    def __init__(self, cls: Series):
        raise NotImplementedError

    @abc.abstractmethod
    def __iter__(self) -> Iterator[Series]:
        """ Iterators over all series contained in the store.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def next_key(self):
        raise NotImplementedError

    @abc.abstractmethod
    def create(self, name: str) -> Series:
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, key: int):
        raise NotImplementedError

    @abc.abstractmethod
    def set(self, key: int, data: bytes):
        raise NotImplementedError

    @abc.abstractmethod
    def add(self, data: bytes) -> int:
        raise NotImplementedError

    def get_series(self, name):
        found = None
        for series in self:
            if series.name() == name:
                found = series
        return found

    def get_series_type(self, name):
        series = self.get_series(name)
        return series.get_type() if series is not None else None

    def get_or_create_series(self, name):
        series = self.get_series(name)
        if series is None:
            series = self.create(name)
            self.notify_changed()
        return series

    def signal_names_and_types(self):
        """ Get a sorted list of signal names. """
        names_and_types = [(series.name(), self.get_series_type(series.name())) for series in self]
        return list(sorted(names_and_types))

    def clear(self):
        """ Remove all signals from the database. """
        raise NotImplementedError()

    # Math operation!
    def add_function(self, name, expr):
        # TODO: name clash?
        #assert name not in self._traces
        #serie = FuncSerie(self, expr)
        #self._traces[name] = serie
        pass

    # Data insertion functions:
    def add_sample(self, name: str, sample):
        """ Add a single sample to the given series. """
        series = self.get_or_create_series(name)
        series.add_sample(sample)
        self.notify_changed()

    def add_samples(self, name: str, samples):
        """ Add samples to the given series. """
        series = self.get_or_create_series(name)
        for s in samples:
            series.add(s)
        self.notify_changed()

    # Query related functions:
    def query_metrics(self, name: str, timespan=None) -> Aggregation:
        series = self.get_series(name)
        if series:
            return series.query_metrics(selection_timespan=timespan)
        raise ValueError

    def query(self, name: str, timespan: TimeSpan, count: int):
        """ Query the database on the given signal.
        """
        series = self.get_series(name)
        if series:
            return series.query(timespan, count)
        raise ValueError

    def query_value(self, name, timestamp):
        series = self.get_series(name)
        if series:
            return series.query_value(timestamp)
        raise ValueError

    # Change handlers
    def register_changed_callback(self, callback):
        self._callbacks.append(callback)
        self._tokens += 1

    def insert_token(self):
        self._tokens += 1
        if self._event_backlog:
            self._event_backlog = False
            self._tokens -= 1
            for callback in self._callbacks:
                callback()

    def notify_changed(self):
        """ Notify listeners of a change.

        Rate limit the events to prevent GUI flooding.
        To do this, keep a token counter, if there is
        an event, check the tokens, if there is a token,
        propagate the event. Otherwise, store the event
        for later processing.

        If more events arrive, aggregate the events into
        a resulting event.
        """
        if self._tokens > 0:
            self._tokens -= 1
            for callback in self._callbacks:
                callback()
        else:
            # Simplest event aggregation: there was an event
            self._event_backlog = True

