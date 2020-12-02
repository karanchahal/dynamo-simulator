"""
This file spawns n processes as dynamo nodes. A client can contact any of these nodes and save/get a database.
"""

import attr
from typing import List, Tuple, Dict
import grpc
from concurrent import futures
from dynamo_pb2_grpc import add_DynamoInterfaceServicer_to_server
from threading import Thread
from structures import Params, Process, KeyValPair, VectorClock
from dynamo_node import DynamoNode


def start_process(n_id, port, view, positions):
    SERVER_ADDRESS = f"localhost:{port}"
    server = grpc.server(futures.ThreadPoolExecutor())
    add_DynamoInterfaceServicer_to_server(DynamoNode(n_id=n_id, view=view, positions=positions), server)

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
    'positions' : [ [100, 9000] , [200, 4000], [500, 10000], [800, 25000] ], # TODO: make random, determinsitic for testing
}

start_db(Params(params))
