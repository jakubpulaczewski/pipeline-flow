from enum import Enum, auto

class ETLStages(Enum):
    extract = auto()
    load = auto()
    transform = auto()


