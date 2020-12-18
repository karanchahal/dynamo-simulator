
from typing import Dict, List, Tuple
from dynamo_pb2 import PutRequest

class VectorClock(object):
    '''
    Vector CLock
    '''
    clock : List[Tuple[int, int]] = None

    def __lt__(self, other):
        for other_item in other.clock:
            for item in self.clock:
                if other_item[0] == item[0] and other_item[1] < item[1]:
                    return False
        return True

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
    The params that specify the dynamo instance configuration
    '''
    def __init__(self, d):
        self.num_proc: int = None # number of dynamo instances in ring
        self.Q: int = None # size of virtual node: should be a power of 2
        self.hash_size: int =  None # number of bits in the hash of the key: hence key space in ring = 2^{hash_size}
        self.N: int = None # the size of the preference list
        self.R: int = None # the number of successful read requests needed
        self.W: int = None # the number of succ write request needed
        self.w_timeout: int = None # the number of seconds the timeout for write replication is
        self.r_timeout: int = None # the number of seconds the timeout for read replication is
        self.gossip: bool = None # turn on gossip or not
        self.update_failure_on_rpcs: bool = True # on failure of non gossip calls, update unhealthy nodes ?
        self.gossip_update_time: Tuple[float, float] = (1, 2)
        self.__dict__ = d

    def __repr__(self):
        print("Loading config..")
        for k, v in self.__dict__.items():
            if k is not None and v is not None:
                print(f"{k} : {v}")
        return ""

class NetworkParams:
    '''
    The params that specify the network configuration
    - latency
    - package drop probability
    '''
    def __init__(self, d=None):
        self.latency: int = 0 # Maximum latency for one request
        self.randomize_latency: bool = False # A parameter to randomize the latency (uniformly between 0 - latency)
        self.drop_prob: float = 0
        if d is not None:
            self.__dict__ = d

    def __repr__(self):
        print("NetworkParams...")
        for k, v in self.__dict__.items():
            if k is not None and v is not None:
                print(f"{k} : {v}")
        return ""

class FutureInformation(object):
    '''
    This object contains information about the future
    '''

    def __init__(self, req: PutRequest, hinted_handoff: int, original_node: int):
        self.req = req # can be a get request as well
        self.hinted_handoff = hinted_handoff # indicates wherether this is a hinted handoff from a certain node
        self.original_node = original_node # node the future was sent to
