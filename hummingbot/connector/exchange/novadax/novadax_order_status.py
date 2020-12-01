from enum import Enum


class NovadaxOrderStatus(Enum):
    pending         = 0
    SUBMITTED       = 10
    PROCESSING      = 100
    PARTIAL_FILLED  = 110
    CANCELING       = 120
    done            = 200
    FILLED          = 210
    failed          = 300
    CANCELED        = 310
    REJECTED        = 400

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
