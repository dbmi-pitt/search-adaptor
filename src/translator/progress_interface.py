import abc


class ProgressReadInterface(metaclass=abc.ABCMeta):
    @property
    def is_indexing(self) -> bool:
        raise NotImplementedError

    @property
    def percent_complete(self) -> int:
        raise NotImplementedError
