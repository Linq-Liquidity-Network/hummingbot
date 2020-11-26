from enum import Enum


class OrderState(Enum):
    UNSENT = 100
    PENDING = 200
    ACTIVE = 300
    PENDING_CANCEL = 400
    # Only states where the order has been confirmed to have been removed from the book should be below this point
    COMPLETE = 500
    CANCELED = 600
    FAILED = 700

    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.value >= other.value
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.value <= other.value
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented
