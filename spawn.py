"""
This file spawns n processes as dynamo nodes. A client can contact any of these nodes and save/get a database.
"""

import attr
from typing import List, Tuple, Dict
import grpc
from concurrent import futures
import dynamo_pb2_grpc
import dynamo_pb2
from threading import Thread


class Process:

    def __init__(self, ip, port, server):
        self.ip : str = ip
        self.port: int = port
        self.server = server


class Params:

    def __init__(self, d):
        self.num_proc: int = None
        self.positions = None
        self.__dict__ = d

    def __repr__(self):
        print("Loading config..")
        for k, v in self.__dict__.items():
            if k is not None and v is not None:
                print(f"{k} : {v}")
        return ""


SERVER_ID = 1


class VectorClock(object):
    clock : List[Tuple[int, int]] = None

class KeyValPair(object):
    key: int = None
    val: str = None 
    context: VectorClock = None

def get_preference_list(node_id):
    pass

class DynamoNode(dynamo_pb2_grpc.DynamoInterfaceServicer):
    """
    This class is the entry point to a dynamo node.
    """
    def __init__(self, n_id: int = None, view: Dict[int, str] = None, positions : List[int] = None):
        super().__init__()

        self.id = n_id

        # positions in ring
        self.positions = positions

        # list of address to other nodes, indexed by node id
        self.view = view

        # a list of > N nodes that are closest to current node, (clockwise)
        self.preference_list = get_preference_list(node_id=id)

        # in memory data store of key, values
        self.memory: List[KeyValPair] = []


    def Get(self, request, context):
        """
        Get request
        """
        print(f"get called by client({request.client_id}) for key: {request.key}")
        response = dynamo_pb2.GetResponse(
            server_id=SERVER_ID,
            val="1",
            context="None",
            metadata="Yo")
        return response


    def Put(self, request, context):
        """
        Put Request
        """
        print(f"put called by client({request.client_id}) for key: {request.key}")

        # add to memory
        self._add_to_memory(request)

        response = dynamo_pb2.PutResponse(
            server_id=SERVER_ID,
            metadata="Success ?")
        return response
    
    def _add_to_memory(self, request):
        # get coordinator node

        # walk up clockwise to find latest node

        raise NotImplementedError



def start_process(n_id, port, view, positions):
    SERVER_ADDRESS = f"localhost:{port}"
    server = grpc.server(futures.ThreadPoolExecutor())
    dynamo_pb2_grpc.add_DynamoInterfaceServicer_to_server(DynamoNode(n_id=n_id, view=view, positions=positions), server)

    server.add_insecure_port(SERVER_ADDRESS)
    print(f"------------------start Python GRPC server at {SERVER_ADDRESS}")
    server.start()

    return Process(ip="localhost", port=port, server=server)

def create_view(start_port, num_proc):
    view = {}
    for i in range(num_proc):
        view[i] = start_port
        start_port += 1
    
    return view


def start_db(params: Params):
    """
    Spawns n servers in different threads and these servers act as dynamo instances
    TODO: convert to processes.
    """
    port = 2333
    processes : List[Process] = []
    view = create_view(start_port=port, num_proc=params.num_proc)
    for i in range(params.num_proc):
        process = start_process(i, view[i], view, params.positions[i])
        processes.append(process)

    # ending condition
    processes[-1].server.wait_for_termination()

    return processes


params = {
    'num_proc' : 4,
    'positions' : [ [100, 9000] , [200, 4000], [500, 10000], [800, 25000] ],

}

start_db(Params(params))
