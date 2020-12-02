
from typing import Dict, List, Tuple

class VectorClock(object):
    '''
    Vector CLock
    '''
    clock : List[Tuple[int, int]] = None

class KeyValPair(object):
    '''
    Key Value Pair for a put/get request
    '''
    key: int = None
    val: str = None 
    context: VectorClock = None

class Process:
    '''
    All the important information to identify a dynamo instance
    '''
    def __init__(self, ip, port, server):
        self.ip : str = ip
        self.port: int = port
        self.server = server

class Params:
    '''
    The params that docate the dynamo instance configuration
    '''
    def __init__(self, d):
        self.num_proc: int = None # number of dynamo instances in ring
        self.Q: int = None # size of virtual node: should be a power of 2
        self.hash_size: int =  None # number of bits in the hash of the key: hence key space in ring = 2^{hash_size}
        self.N: int = None # the size of the preference list
        self.__dict__ = d

    def __repr__(self):
        print("Loading config..")
        for k, v in self.__dict__.items():
            if k is not None and v is not None:
                print(f"{k} : {v}")
        return ""
