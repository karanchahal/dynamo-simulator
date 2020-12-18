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
import time
import multiprocessing
import datetime


import logging


_ONE_DAY = datetime.timedelta(days=1)

def _wait_forever(server):
    try:
        while True:
            time.sleep(_ONE_DAY.total_seconds())
    except KeyboardInterrupt:
        server.stop(None)


def start_process_multiprocess(n_id, port, view, membership_information, params, network_params, logger):
    logger.info(f"Starting process {n_id} at port {port} with params: {params}")
    SERVER_ADDRESS = f"localhost:{port}"
    server = grpc.server(futures.ThreadPoolExecutor())
    add_DynamoInterfaceServicer_to_server(DynamoNode(
                                n_id=n_id,
                                view=view,
                                membership_information=membership_information, 
                                params=params,
                                network_params=network_params,
                                logger=logger),
                            server)

    server.add_insecure_port(SERVER_ADDRESS)
    logger.info(f"Python GRPC server running at {SERVER_ADDRESS}")
    server.start()

    _wait_forever(server)



def start_process(n_id, port, view, membership_information, params, network_params):
    logger.info(f"Starting process {n_id} at port {port} with params: {params}")
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
    logger.info(f"Python GRPC server running at {SERVER_ADDRESS}")

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
    logger.info(f"Membership Info {membership_information} Number of processes {params.num_proc}")
    for i in range(params.num_proc):
        process = start_process(i, view[i], view, membership_information, params, network_params)
        processes.append(process)

    # ending condition
    if wait:
        processes[-1].server.wait_for_termination()

    return processes

def start_db_multiprocess(params: Params, membership_information: Dict[int, List[int]], network_params: NetworkParams = None, wait=False, start_port: int = 2333, logger = None):
    """
    Spawns n servers in different threads and these servers act as dynamo instances
    TODO: convert to processes.
    """
    processes : List[Process] = []
    view = create_view(start_port=start_port, num_proc=params.num_proc)
    logger.info(f"Membership Info {membership_information} Number of processes {params.num_proc}")
    for i in range(params.num_proc):
        process = multiprocessing.Process(target=start_process_multiprocess, args=(i, view[i], view, membership_information, params, network_params, logger))
        process.start()
        processes.append(process)

    for p in processes:
        p.join()

def start_db_background(params: Params, membership_information: Dict[int, List[int]], network_params: NetworkParams, num_tasks:int = 2, wait: bool = False, start_port: int = 2333):
    executor = futures.ThreadPoolExecutor(max_workers=num_tasks)
    server = executor.submit(start_db, params, membership_information, network_params, wait, start_port)
    
    return server

def start_db_background_multiprocess(params: Params, membership_information: Dict[int, List[int]], network_params: NetworkParams, logger, num_tasks:int = 2, wait: bool = False, start_port: int = 2333):
    executor = futures.ThreadPoolExecutor(max_workers=num_tasks)
    server = executor.submit(start_db_multiprocess, params, membership_information, network_params, wait, start_port, logger)
    return server



def init_server(params, network_params):

    membership_information = init_membership_list(params)

    logging.basicConfig(filename='dynamo_node.log', level=logging.DEBUG)
    logger = logging.getLogger('dynamo_node')
    logger.setLevel(logging.DEBUG)

    logger.debug("Testing...")

    start_db_background_multiprocess(params, membership_information, network_params, logger)
    # start_db_background(params, membership_information, network_params, wait=True, logger=logger)
