"""
This file spawns n processes as dynamo nodes. A client can contact any of these nodes and save/get a database.
"""

import attr
from typing import List, Tuple, Dict
import grpc
from concurrent import futures
from dynamo_pb2_grpc import add_DynamoInterfaceServicer_to_server
from threading import Thread
from structures import Params, Process, NetworkParams
from dynamo_node import DynamoNode
from partitioning import init_membership_list


def start_process(n_id, port, view, membership_information, params, network_params):
    SERVER_ADDRESS = f"localhost:{port}"
    server = grpc.server(futures.ThreadPoolExecutor())
    add_DynamoInterfaceServicer_to_server(DynamoNode(
                                n_id=n_id,
                                view=view,
                                membership_information=membership_information, 
                                params=params,
                                network_params=network_params), 
                            server)

    server.add_insecure_port(SERVER_ADDRESS)
    print(f"------------------start Python GRPC server at {SERVER_ADDRESS}")
    server.start()

    return Process(ip="localhost", port=port, server=server)

def create_view(start_port, num_proc) -> Dict[int, int]:
    '''
    Get key value pair to each process in the dynamo ring.
    Returns address to each instance.
    '''
    view = {}
    for i in range(num_proc):
        view[i] = start_port
        start_port += 1
    
    return view


def start_db(params: Params, membership_information: Dict[int, List[int]], network_params: NetworkParams = None, wait=False, start_port: int = 2333):
    """
    Spawns n servers in different threads and these servers act as dynamo instances
    TODO: convert to processes.
    """
    processes : List[Process] = []
    view = create_view(start_port=start_port, num_proc=params.num_proc)
    print(f"Membership Info {membership_information}")
    for i in range(params.num_proc):
        process = start_process(i, view[i], view, membership_information, params, network_params)
        processes.append(process)

    # ending condition
    if wait:
        processes[-1].server.wait_for_termination()

    return processes

def start_db_background(params: Params, membership_information: Dict[int, List[int]], network_params: NetworkParams, num_tasks:int = 2, wait: bool = False, start_port: int = 2333):
    executor = futures.ThreadPoolExecutor(max_workers=num_tasks)
    server = executor.submit(start_db, params, membership_information, network_params, wait, start_port)
    return server

def init_server():
    params = {
        'num_proc' : 4,
        'hash_size': 3, # 2^3 = 8 
        'Q' : 2, # 
        'N' : 2
    }
    params = Params(params)
    membership_information = init_membership_list(params)

    network_params = {
        'latency': 10,
        'randomize_latency': False,
        'drop_prob': 0
    }
    network_params = NetworkParams(network_params)
    start_db(params, membership_information, network_params)

# init_server()