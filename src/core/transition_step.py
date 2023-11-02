""" A Transition step to move data from one stage to next one i.e. 
    from Extract To Transform, and Transform to Load. """
from typing import Protocol

class TransitionInterface(Protocol):
    ...

    def store(self): # TODO: Potentially change it.
        ...

class DataStorage:
    ...

    def store(self): # TODO: Potentially change it.
        ...

class MethodCall:
    def store(self): # TODO: Potentially change it.
        ...
    

class Queue:
    def store(self): # TODO: Potentially change it.
        ...