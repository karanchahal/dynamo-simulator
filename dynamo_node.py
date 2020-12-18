
from dynamo_pb2_grpc import DynamoInterfaceServicer
from typing import List, Tuple, Dict, Union
from dynamo_pb2 import PutResponse, GetResponse, PutRequest, GetRequest, ReplicateResponse, MemResponse, ReadResponse, ReadItem, Memory, VectorClockItem, FailRequest, HeartbeatRequest, DataBunchRequest, DataBunchResponse
from partitioning import createtoken2node, find_owner, get_preference_list_for_token, get_ranges
from structures import NetworkParams, Params, FutureInformation
from typing import List, Tuple, Dict, Union, Set
from dynamo_pb2_grpc import DynamoInterfaceStub
import grpc
import threading
import time
import concurrent
import random
import time
import sys
import logging


def replicate_rpc(view, rep_n, request) -> ReplicateResponse:
    """
    Send a RPC call to a process telling it to replicate the request in it's memory
    """
    port = view[rep_n]
    with grpc.insecure_channel(f"localhost:{port}") as channel:
        stub = DynamoInterfaceStub(channel)
        response = stub.Replicate(request)
    return response

def read_rpc(view, rep_n, request) -> ReadResponse:
    """
    Send a RPC call to a process telling it to read the data corresponding to a key
    """
    port = view[rep_n]
    with grpc.insecure_channel(f"localhost:{port}") as channel:
        stub = DynamoInterfaceStub(channel)
        response = stub.Read(request)
    return response

def heartbeat_rpc(view: Dict[int, int], rep_n: int, request: HeartbeatRequest) -> HeartbeatRequest:
    """
    Send a RPC call to a process telling it to read the data corresponding to a key
    """
    port = view[rep_n]
    with grpc.insecure_channel(f"localhost:{port}") as channel:
        stub = DynamoInterfaceStub(channel)
        response = stub.Heartbeat(request)
    return response

def transfer_data_rpc(view: Dict[int, int], rep_n: int, request: DataBunchRequest) -> DataBunchResponse:
    """
    Send a RPC call to a process telling it to add the following data to it's memory buffer
    """
    port = view[rep_n]
    with grpc.insecure_channel(f"localhost:{port}") as channel:
        stub = DynamoInterfaceStub(channel)
        response = stub.TransferData(request)
    return response

class DynamoNode(DynamoInterfaceServicer):
    """
    This class is the entry point to a dynamo node.
    """
    def __init__(self, n_id: int, view: Dict[int, str], 
        membership_information: Dict[int, List[int]], params: Params,
        network_params: NetworkParams, logger: logging.Logger):
        """
        view: dict indexed by node id, value is the address of the node
        membership_information: dict indexed by node id, gives set of tokens for each node. 
            Each token states the starting point of a virtual node which is of size `Q` (see `Params`).
        params: Stores config information of the dynamo ring
        """
        super().__init__()

        self.params = params
        self.network_params = network_params
        self.logger = logger

        self.n_id = n_id

        # tokens given to various nodes in the ring: bootstrapped information
        self.membership_information = membership_information

        # list of address to other nodes, indexed by node id
        self.view = view

        # whether set node to fail
        self.fail = False

        self.token2node = createtoken2node(membership_information)

        # in memory data store of key, values
        self.memory_of_node_lock = threading.Lock()
        self.memory_of_node: Dict[int, PutRequest] = {}

        self.memory_of_replicas_lock = threading.Lock()
        self.memory_of_replicas: Dict[int, Memory] = {}

        # local view of failed nodes
        self.failed_node_lock = threading.Lock()
        self.failed_nodes: Set[int] = set({})

        # keep track of all tokems used by a request
        self.tokens_used: Dict[int, List[int]] = {}

        # gossip protocol
        if self.params.gossip == True:
            self.start_gossip_protocol()
        

    def scan_and_send(self, n_id):
        self.logger.debug(f"[scan_and_send] in this function at {self.n_id} sending info to {n_id} Init....")
        vals_to_send = []
        self.memory_of_replicas_lock.acquire()
        for k, mem_of_n in self.memory_of_replicas.items():
            # iterate over memory of all n's
            for key, val in mem_of_n.mem.items():
                if val.hinted_handoff == n_id:
                    vals_to_send.append(val)
        self.memory_of_replicas_lock.release()
        if vals_to_send == []:
            # nothing to send
            return
        self.logger.debug(f"--------Vals to send ? {vals_to_send}")
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        request = DataBunchRequest(sent_from=self.n_id, requests=vals_to_send)
        self.logger.debug(f"[scan_and_send] Sending request.... in this function at {self.n_id} sending info to {n_id}")

        def catch_callback(fut):
            """
            If future returns successfully, then we can go ahead and remove the extra
            stuff from the local memory
            """
            nonlocal vals_to_send

            if fut.exception() or fut.result().succ == False:
                self.logger.error(f"Error received in [Tranferring Data], reverse health of the node {n_id}")
                self.update_failed_nodes(n_id)
            else:
                self.logger.info(f"Success in transferring data at node {n_id}, we can delete our data now...")
                to_del = []
                self.memory_of_replicas_lock.acquire()
                for k, mem_of_n in self.memory_of_replicas.items():
                    # iterate over memory of all n's
                    for key, val in mem_of_n.mem.items():
                        if val in vals_to_send:
                            to_del.append((k, key))
                
                for k1,k2 in to_del:
                    del self.memory_of_replicas[k1].mem[k2]
                
                self.logger.info(f"Deletion of {n_id}'s hinted handoff information successful")
                self.memory_of_replicas_lock.release()



        fut = executor.submit(transfer_data_rpc, self.view, n_id, request)
        fut.add_done_callback(catch_callback)
        

    
    def start_gossip_protocol(self):
        # start a thread in the background that pings random nodes to determine health
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        mem_lock = threading.Lock()
        fut2req = {}
        def get_heartbeat(fut):
            self.logger.debug(f"[get_heartbeat] in this function at {self.n_id} initing...")
            nonlocal mem_lock
            nonlocal fut2req

            if fut.exception():
                
                # heartbeat failed, add node to unhealthy
                self.logger.debug(f"[get_heartbeat] exception !")
                mem_lock.acquire()
                bad_n_id = fut2req[fut]
                mem_lock.release()
                self.logger.error(f"Future at node {self.n_id}/{self.view[self.n_id]} for node {bad_n_id}/{self.view[bad_n_id]} had exception {fut.exception()}") 
                self.update_failed_nodes(bad_n_id, remove=False)
            else:
                # all good with node, do nothing !
                self.logger.debug(f"[get_heartbeat] all good, init for scanning and sending !")
                res = fut.result()
                if res.succ != True:
                    # all bad
                    self.logger.debug(f"[get_heartbeat] all good, bad node updating health")
                    self.update_failed_nodes(res.sent_to, remove=False)
                else:
                    self.logger.debug(f"[get_heartbeat] all good, good node updating health")
                    self.update_failed_nodes(res.sent_to, remove=True)
                    # do scan in replica memory and send it to responsible node
                    self.scan_and_send(res.sent_to)
                self.logger.debug(f"[get_heartbeat] all good path, everything done !")
            
            # delete fut from dict, to prevent memeory leak
            mem_lock.acquire()
            del fut2req[fut]
            mem_lock.release()
            self.logger.debug(f"[get_heartbeat] in this function at {self.n_id} wrapped up everything.")


        def ping_random_node():
            nonlocal executor
            nonlocal mem_lock
            nonlocal fut2req

            while True:
                self.logger.debug(f"[ping_random_node] {self.n_id} Trying to send another heartbeat !")
                time.sleep(random.uniform(self.params.gossip_update_time[0], self.params.gossip_update_time[1]))

                if not self.fail:
                    n_id = self.n_id
                    while n_id == self.n_id:
                        n_id = random.randint(0,self.params.num_proc-1)
                    
                    request = HeartbeatRequest(sent_to=n_id, from_n=self.n_id)
                    fut = executor.submit(heartbeat_rpc, self.view, n_id, request)

                    fut.add_done_callback(get_heartbeat)

                    mem_lock.acquire()
                    fut2req[fut] = n_id
                    mem_lock.release()

        fut = executor.submit(ping_random_node)

    def TransferData(self, request: DataBunchRequest, context):
        """
        Adds all data to it's replica buffer or it's memory buffer ?
        1. if data is being transferred, then assumption is that it will be transferred to replica mem
        2. If coordinator node goes down, then some other node should take it's place, with hinted handoff.

        For each data item, check if key makes it current nodes owner, if not, then store in replic memory
        
        TODO: add locks
        """
        self.logger.info(f"[TransferData] at node = {self.n_id} at port {self.view[self.n_id]}")
        self._check_add_latency()
        if self.fail:
            self.logger.error(f"Node {self.n_id} at {self.view[self.n_id]} is set to fail, fail it !")
            raise concurrent.futures.CancelledError # retirning None will result in failure
        
        try:
            all_reqs = request.requests

            for req in all_reqs:
                key = req.key
                req.hinted_handoff = -1
                n_id = find_owner(key, self.params, self.token2node)
                if n_id != self.n_id:
                    self._add_to_hash_table(req, add_to_replica=True, coord_id=n_id)
                else:
                    self._add_to_hash_table(req)
            
            return DataBunchResponse(sent_from=self.n_id, succ=True)

        except:
            self.logger.error(f"Error with [TransferData] at node = {self.n_id} at port {self.view[self.n_id]}")
            return DataBunchResponse(sent_from=self.n_id, succ=False)
            
        


    def Get(self, request: GetRequest, context):
        """
        Get request
        """
        self.logger.info(f"[GET] called by client {request.client_id} for key: {request.key} at node {self.n_id} at port {self.view[self.n_id]}")
        self._check_add_latency()
        if self.fail:
            self.logger.error(f"Node {self.n_id} at {self.view[self.n_id]} is set to fail, fail it !")
            raise concurrent.futures.CancelledError # retirning None will result in failure

        response: GetResponse = self._get_from_memory(request)
        return response
    
    def Heartbeat(self, request: GetRequest, context):
        """
        Get request
        """
        self.logger.info(f"[Heartbeat] called by client {request.from_n} at node {self.n_id} at port {self.view[self.n_id]}")
        self._check_add_latency()
        if self.fail:
            self.logger.error(f"Node {self.n_id} at {self.view[self.n_id]} is set to fail, fail it !")
            raise concurrent.futures.CancelledError # retirning None will result in failure
        
        request.succ = True
        return request

    def Read(self, request: GetRequest, context):
        """
        Read request

        Assumes current node has key
        """
        self.logger.info(f"[Read] called for key {request.key} at node {self.n_id} at port {self.view[self.n_id]}")
        self._check_add_latency()
        if self.fail:
            self.logger.error(f"Node {self.n_id} at {self.view[self.n_id]} is set to fail, fail it !")
            raise concurrent.futures.CancelledError # retirning None will result in failure
        # try:
        req_token = request.key // self.params.Q
        node = self.token2node[req_token]
        if self.n_id == node:
            response = self._get_from_hash_table(request.key)
        else:  
            response = self._get_from_hash_table(request.key, coord_nid=request.coord_nid, from_replica=True)
        return response
        # except:
        #     self.logger.error(f"[Exception in READ] for Key={request.key} at Nid/Port: {self.n_id}/{self.view[self.n_id]} | Owner of this key is {node} |  Exception is {sys.exc_info()[0]}")
        #     response: ReadResponse = ReadResponse(succ=False)
        return response

    def Put(self, request: PutRequest , context):
        """
        Put Request
        """
        self.logger.info(f"[Put] called for key {request.key} at node {self.n_id} at port {self.view[self.n_id]}")
        self._check_add_latency()
        if self.fail:
            self.logger.error(f"Node {self.n_id} is set to fail")
            raise concurrent.futures.CancelledError # retirning None will result in failure
        try:
            # add to memory
            response: PutResponse = self._add_to_memory(request, request_type="put")

            self.logger.info(f"[Put] sending a response back {response}")
        except:
            self.logger.error(f"[Exception in Put] for Key={request.key} at Nid/Port: {self.n_id}/{self.view[self.n_id]} |  Exception is {sys.exc_info()[0]} ")
            response = PutResponse(succ=False)
        return response
    
    def Replicate(self, request: PutRequest , context):
        """
        Replicate Request
        """
        self.logger.info(f"[Replicate] called for key {request.key} at node {self.n_id}")
        self._check_add_latency()

        if self.fail:
            self.logger.error(f"Node {self.n_id} is set to fail, fail it !")
            raise concurrent.futures.CancelledError # retirning None will result in failure
        
        # find token for key
        req_token = request.key // self.params.Q

        # find node for token
        node = self.token2node[req_token]

        # add to memory
        if self.n_id == node:
            self._add_to_hash_table(request)
        else:
            self._add_to_hash_table(request, add_to_replica=True, coord_id=request.coord_nid)
        # construct replicate response
        response = ReplicateResponse(server_id=self.n_id, 
                    metadata="Replication successful", 
                    succ=True)
             
        return response

    def _get_from_memory(self, request: GetRequest):
        """
        Tries to read from the memory of the node if the key is in the range of the current node.
        If not, the request the rerouted to the appropriate node.

        Returns a GetResponse
        """
        self.logger.debug(f"1 {self.identity(request)}| In [_get_from_memory]")
        key = request.key
        self.logger.debug(f"2 {self.identity(request)}| In [_get_from_memory]")
        # find token for key
        req_token = key // self.params.Q
        self.logger.debug(f"3 {self.identity(request)}| In [_get_from_memory]")
        # find node for token
        node = self.token2node[req_token]
        self.logger.debug(f"4 {self.identity(request)}| In [_get_from_memory]")
        pref_list_for_token, node_list = self.get_top_N_healthy_pref_list(req_token)
        self.logger.debug(f"5 {self.identity(request)}| In [_get_from_memory]")
        # if curr node is not the coordinator
        if self.n_id not in node_list:
            # this request needs to be rerouted to first node in nodes
            self.logger.debug(f"6 {self.identity(request)}| In [_get_from_memory]")
            return self.reroute(node, "get")
        self.logger.debug(f"7 {self.identity(request)}| In [_get_from_memory]")
        # if curr node is coordinator node
        response = self.read(request, req_token)
        self.logger.debug(f"8 {self.identity(request)}| In [_get_from_memory]")
        return response

    def _get_from_hash_table(self, key: int, coord_nid:int = None, from_replica: bool=False):
        """
        Used by the get/read requests and add it to the hash table
        Returns succ code, and request if succis True else returns None
        """
        if from_replica:
            self.logger.debug(f"[GET] Here10: {key} | {self.n_id} | {self.view[self.n_id]}")
            self.memory_of_replicas_lock.acquire()
            if coord_nid not in self.memory_of_replicas:
                self.memory_of_replicas_lock.release()
                self.logger.debug(f"[GET] Heregood: {key} | {self.n_id} | {self.view[self.n_id]}")
                return ReadResponse(server_id=self.n_id, succ=False, metadata=f"No replica memory for node {coord_nid} in {self.n_id}")

            memory = self.memory_of_replicas[coord_nid].mem

            if key not in memory:
                self.memory_of_replicas_lock.release()
                self.logger.debug(f"[GET] Heregood: {key} | {self.n_id} | {self.view[self.n_id]}")
                return ReadResponse(server_id=self.n_id, succ=False, metadata=f"No key {key} in replica memory for node {coord_nid} in {self.n_id}")

            put_request = memory[key]
            self.memory_of_replicas_lock.release()
            self.logger.debug(f"[GET] Here14: {key} | {self.n_id} | {self.view[self.n_id]}")
        else:
            self.logger.debug(f"[GET] Here11: {key} | {self.n_id} | {self.view[self.n_id]}")
            self.memory_of_node_lock.acquire()
            memory = self.memory_of_node
            if key not in memory:
                self.memory_of_node_lock.release()
                self.logger.debug(f"[GET] Heregood: {key} | {self.n_id} | {self.view[self.n_id]}")
                return ReadResponse(server_id=self.n_id, succ=False, metadata=f"No key {key} in main memory for node {self.n_id}")

            put_request = memory[key]
            self.memory_of_node_lock.release()
            self.logger.debug(f"[GET] Here15: {key} | {self.n_id} | {self.view[self.n_id]}")

        return ReadResponse(server_id=self.n_id,
            item=ReadItem(val=put_request.val, context=put_request.context),
            metadata="success", succ=True)

    def _add_to_hash_table(self, request, add_to_replica=False, coord_id=None):
        """
        Adds request to in memory hash table.
        """
        if not add_to_replica:
            self.logger.debug(f"[_add_to_hash_table] Adding to node memory: {request.key} | {self.n_id} | {self.view[self.n_id]}")
            self.memory_of_node_lock.acquire()
            self.memory_of_node[request.key] = request
            self.memory_of_node_lock.release()
        else:
            self.logger.debug(f"[_add_to_hash_table] Adding to replica memory: {request.key} | {self.n_id} | {self.view[self.n_id]}")
            self.memory_of_replicas_lock.acquire()
            if coord_id not in self.memory_of_replicas:
                self.memory_of_replicas[coord_id] = Memory(mem={
                    request.key: request
                })
            else:
                mem_dict = dict(self.memory_of_replicas[coord_id].mem)
                mem_dict[request.key] = request
                self.memory_of_replicas[coord_id] = Memory(mem=mem_dict)
            self.memory_of_replicas_lock.release()


    def _add_to_memory(self, request, request_type: str):
        """
        Adds to the memory of the node if the key is in the range of the current node.
        If not the request is rerouted to the appropriate node.

        1. get coordinator node
        2. search for closest node
        3. walk up clockwise to find latest node

        Returns a GetResponse or a PutResponse
        """
        key = request.key # assuming this key is in the key space

        # find token for key
        req_token = key // self.params.Q


        # find node for token
        node = self.token2node[req_token]

        pref_list_for_token, node_list = self.get_top_N_healthy_pref_list(req_token)

        # if curr node is not the coordinator
        if self.n_id not in node_list:
            # this request needs to be rerouted to first node in nodes
            # get random healthy node in perference list that is not bad
            return self.reroute(node, request_type)

        # if curr node is coordinator node...
        # update context
        self._update_clock(request.context.clock)
        # store it, in replica memory or main memory
        if node == self.n_id:
            self._add_to_hash_table(request)
        else:
            self._add_to_hash_table(request, add_to_replica=True, coord_id=node)

        # send request to all replica nodes
        self.logger.info(f"Replicating.... key={request.key}, val={request.val}")
        response = self.replicate(request, req_token)
        self.logger.info(f"Replication done !")

        # return back to client with 
        return response

    def _filter_latest(self, items: List[ReadItem]):
        """
        Filters the list of ReadItems to return the latest item(s)
        """
        def clock_lt(c1, c2):
            for it2 in c2:
                for it1 in c1:
                    if it2.server_id == it1.server_id and it1.count > it2.count:
                        return False
            return True

        def clock_gt(c1, c2):
            for it2 in c2:
                for it1 in c1:
                    if it2.server_id == it1.server_id and it1.count < it2.count:
                        return False
            return True

        max_clock = items[0].context.clock
        filtered_items = [items[0]]
        for item in items[1:]:
            if clock_gt(item.context.clock, max_clock):
                filtered_items = [item]
                max_clock = item.context.clock
            elif clock_lt(item.context.clock, max_clock):
                pass
            else:
                filtered_items.append(item)
        
        return filtered_items

    def _update_clock(self, clock):
        """
        Updates the vector clock during put request.
        This method should only be called by the coordinator node.
        """
        # See if current server already exists in clock
        for clock_item in clock:
            if clock_item.server_id == str(self.n_id):
                clock_item.count += 1
                return
        # If not found
        clock.append(VectorClockItem(server_id=str(self.n_id), count=1))

    def reroute(self, node: int, request_type: str) -> Union[PutResponse, GetResponse]:
        """
        Reroutes the request, gives a rejection signal to the client,
        informs the client on which node to hit.
        node -> node id
        request_type = (get/put)
        """
        assert(request_type == "get" or request_type == "put")
        self.logger.info(f'Rerouting to {self.view[node]}...')
        if request_type == "put":
            return PutResponse(server_id=self.n_id, metadata="needs to reroute !", reroute=True, reroute_server_id=self.view[node])
        elif request_type == "get":
            return GetResponse(server_id=self.n_id, items=None, metadata="needs to reroute!", reroute=True, reroute_server_id=self.view[node])
        else:
            self.logger.error("Rerouting error, wrong params should be get/put")
            raise NotImplementedError

    def read(self, request, token):
        """
        Read logic for dynamo. Assumes current node is the coordinator node.
        The request to replicate will be made asynchronously and after R replicas have 
        returned successfully, then we can return to client with success.

        Else this function will block until replication is done.
        TODO: add async operation
        """
        self.logger.debug(f"{self.identity(request)}| In [read]")
        # Stores result from all reads to check divergences
        items = []

        # vaiables to deal with failures
        replica_lock = threading.Lock()
        completed_reps = 1
        fut2replica = {}
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        fs = set([])

        # Read data on current node
        req_token = request.key // self.params.Q
        main_node = self.token2node[req_token]

        try:
            if main_node != self.n_id:
                req = self._get_from_hash_table(request.key, coord_nid=main_node, from_replica=True)
            else:
                req = self._get_from_hash_table(request.key)
            
            self.logger.debug(f"{self.identity(request)}| Inside try_catch {req} | {req.item}")
            item = req.item
            items.append(item)
        except:
            self.logger.error(f"[{self.identity(request)}] [Exception in read] for Key={request.key} at Nid/Port: {self.n_id}/{self.view[self.n_id]} | Owner of this key is {main_node} |  Exception is {sys.exc_info()[0]} ")
            response = GetResponse(server_id=self.n_id, metadata="key error ?", succ=False)
            # TODO: can do read repair here
            completed_reps = 0

        items_lock = threading.Lock()

        # Callback to handle success/failure of read_rpc requests
        def get_callback(executor, fut2replica, tokens_used):
            nonlocal completed_reps
            nonlocal items
            def read_rpc_callback(f):
                nonlocal executor
                nonlocal fut2replica
                nonlocal tokens_used
                nonlocal completed_reps

                if self.read_failure(f):
                    # get all information pertinent to future
                    s = time.time()
                    future_information = fut2replica[f]

                    # hinted handoff in get/read
                    req = future_information.req
                    req.hinted_handoff = future_information.hinted_handoff if future_information.hinted_handoff != -1 else future_information.original_node

                    # update failed nodes, fo gossip to overturn in the future
                    failed_node_to_add = future_information.original_node

                    if self.params.update_failure_on_rpcs:
                        self.update_failed_nodes(failed_node_to_add)

                    # get new candidate node from spare list
                    new_n, new_token = self.get_spare_node(tokens_used)

                    if new_n == None:
                        self.logger.debug(f"[read] [{self.identity(request)}] We have gone across the entire ring and discovered no node that can service this request. Giving up....")
                        return
                    # update used tokens memory
                    tokens_used.append(new_token)

                    self.logger.debug(f"[{self.identity(request)}] New node selected for READ hinted handoff is {new_n}, firing request")

                    fut = executor.submit(read_rpc, self.view, new_n, req)

                    # add callback
                    fut.add_done_callback(read_rpc_callback)

                    fut2replica[fut] = FutureInformation(req=req, hinted_handoff=new_n, original_node=future_information.original_node)

                    e = time.time()

                else:
                    replica_lock.acquire()
                    completed_reps += 1
                    replica_lock.release()

                    items_lock.acquire()
                    item = f.result().item
                    items.append(item)
                    items_lock.release()

                    self.logger.info(f"[{self.identity(request)}] [READ callback] Successful {completed_reps} / {self.params.N}")

                    if completed_reps == self.params.N:
                        # this means our request is successfully replicated, we can RIP !
                        self.logger.info(f"[{self.identity(request)}] Get request has been successfully replicated :) ")

            return read_rpc_callback

        pref_list, pref_node_list = self.get_top_N_healthy_pref_list(token)
        callback = get_callback(executor, fut2replica, pref_list)
        self.logger.debug(f"[{self.identity(request)}] [Coordinate READ] Pref list at {self.view[self.n_id]} is {pref_list}")
        for p in pref_node_list:
            # nessesary for storing in replica memory stores
            request.coord_nid = main_node

            if p != self.n_id:
                # assuming no failures
                fut = executor.submit(read_rpc, self.view, p, request)
                # add description of future in a hash table
                fut_info = FutureInformation(req=request, original_node=p, hinted_handoff=-1)
                fut2replica[fut] = fut_info
                # add callback
                fut.add_done_callback(callback)
                fs.add(fut)

        # wait until max timeout, and check if W writes have succeeded, if yes then return else fail request
        itrs = concurrent.futures.as_completed(fs, timeout=self.params.r_timeout)
        failure = False

        if self.params.R > 1:
            try:
                r = 1 # already read from original node
                for it in itrs:
                    if not self.read_failure(it):
                        r += 1
                        self.logger.info(f"[{self.identity(request)}] ITRS: Reads from {r} out of {self.params.R} nodes done !")
                        if r >= self.params.R:
                            self.logger.debug(f"[{self.identity(request)}] [Read coordinator] Breaking out of loop as {r} / {self.params.R} reads finishes")
                            break
                # TODO: store the futures that have not finished
            except concurrent.futures.TimeoutError:
                # time has expired
                failure = True
                self.logger.error(f" [{self.identity(request)}] [Read coordinator] Time has expired !")
                return GetResponse(server_id=self.n_id, items=items, metadata="timeout", reroute=False, reroute_server_id=-1)

        num_wait_times = 0
        while completed_reps < self.params.R and not failure:
            if num_wait_times % 100 == 0:
                self.logger.debug(f"[{self.identity(request)}] We are waiting for read request to pass.... {completed_reps} {self.params.R}")
            time.sleep(0.001)

            num_wait_times += 1

            if num_wait_times > 1000:
                failure = True
                break
        
        if failure:
            # TODO: implement succ or failure of put response
            return GetResponse(server_id=self.n_id, succ=False)

        items_lock.acquire()
        filtered_items = self._filter_latest(items)
        items_lock.release()

        response = GetResponse(server_id=self.n_id, items=filtered_items, metadata="success", reroute=False, reroute_server_id=-1, succ=True)
        return response


    def update_failed_nodes(self, node: int, remove: bool = False):
        """
        Update unhealthyness of a node, (will be reversed by gossip protocol)
        """
        self.logger.debug(f"[update_failed_nodes] {self.n_id} / {self.view[self.n_id]} Updating status of node {node} | remove from failing nodes = {remove} ")
        self.failed_node_lock.acquire()
        if remove:
            if node in self.failed_nodes:
                self.failed_nodes.remove(node)
        else:
            self.failed_nodes.add(node)
        self.failed_node_lock.release()
    
    def get_spare_node(self, tokens_used: List[int]):
        """
        Move up the list and get a node that has not been used by the current request.
        This node will be used in the hinted handoff
        

        Returns [None,None] if there is not new node to be founds to service this request, can't do anything
        """
        # check big list for next node not current being used.
        # need to be careful about what kind of nodes have already been used.
        nodes_used = [self.token2node[t] for t in tokens_used]
        last_token_used = tokens_used[-1]
        key_space = pow(2, self.params.hash_size)
        total_v_nodes = round(key_space / self.params.Q)
        new_token = (last_token_used + 1) % total_v_nodes
        self.logger.debug(f"[get_spare_node] at {self.n_id}/{self.view[self.n_id]}. Tokens used : {tokens_used} | Failed nodes: {self.failed_nodes}")
        # move up clockwise
        token_used = None
        while new_token != last_token_used:
            new_node = self.token2node[new_token]
            self.logger.debug(f"[get_spare_node] at {self.n_id}/{self.view[self.n_id]}. New token candidate {new_token} | New node {new_node} | tokens used {tokens_used} | nodes used {nodes_used}")
            self.failed_node_lock.acquire()
            if new_node not in nodes_used and new_node not in self.failed_nodes:
                self.logger.debug(f"[get_spare_node] at {self.n_id}/ {self.view[self.n_id]}. Success ! Token selected {new_token} | Node selected {new_node} | tokens used {tokens_used} | nodes used {nodes_used}")
                self.failed_node_lock.release()
                token_used = new_token
                return new_node, token_used
            self.failed_node_lock.release()
            new_token = (new_token + 1) % total_v_nodes
        
        self.logger.error(f"[get_spare_node] at {self.n_id}/{self.view[self.n_id]}. No spare tokens found, no new nodes to deliver request")
        # TODO: handle gracefully
        return None, None

    def get_top_N_healthy_pref_list(self, token):
        """
        If node is unhealthy in ring, then go ahead in ring until we get to healthy node
        """
        self.failed_node_lock.acquire()
        pref_list, node_list = get_preference_list_for_token(token=token, token2node=self.token2node, params=self.params, unhealthy_nodes=self.failed_nodes)
        self.failed_node_lock.release()
        return pref_list, node_list

    def read_failure(self, fut):
        ret = fut.exception() is None
        if not fut.exception():
            res = fut.result()
            if res.succ:
                return False
        return True

    def identity(self, request):
        return f"Node/Port={self.n_id}/{self.view[self.n_id]} | Key: {request.key}"

    def replicate(self, request, token):
        """
        Replication logic for dynamo DB. Assumes current node is the coordinator node.
        The request to replicate will be made asynchronously and after R replicas have 
        returned successfully, then we can return to client with success.

        Else this function will block until replication is done.
        """
        replica_lock = threading.Lock()
        completed_reps = 1
        fut2replica = {}
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        fs = set([])

        def get_callback(executor, fut2replica, tokens_used):
            # logger.info(f"Completed_reps !! {completed_reps}")
            nonlocal completed_reps
            def write_rpc_callback(f):
                """
                If future succeeds great !
                If future does not succeed, we have to
                    1. find a spare node, send the request there with hinted handoff.
                    2. mark node as unhealthy, gossip will mark it as healthy later on.
                    3. once all N request have been replicated, we can rest in peace.
                    4. store a global variable in higher order function, inc it until N
                """
                nonlocal executor
                nonlocal fut2replica
                nonlocal completed_reps
                nonlocal tokens_used
                if f.exception():

                    # get all information pertinent to future
                    future_information = fut2replica[f]

                    # add hinted handoff information
                    req = future_information.req
                    req.hinted_handoff = future_information.original_node

                    self.logger.debug(f"[{self.identity(request)}] Hinted handoff request is {req}")

                    failed_node_to_add = future_information.hinted_handoff if future_information.hinted_handoff != -1 else future_information.original_node

                    self.logger.debug(f"[{self.identity(request)}] Failed node is {failed_node_to_add}")
                    if self.params.update_failure_on_rpcs:
                        self.update_failed_nodes(failed_node_to_add)

                    # get new candidate node from spare list
                    new_n, new_token = self.get_spare_node(tokens_used)

                    if new_n == None:
                        self.logger.debug(f"[replicate] [{self.identity(request)}] We have gone across the entire ring and discovered no node that can service this request. Giving up....")
                        return

                    # update used tokens memory TODO: should add a lock
                    tokens_used.append(new_token)

                    self.logger.debug(f"[{self.identity(request)}] New node selected for hinted handoff is {new_n}")

                    # make the call, add the current function so that it's called again
                    fut = executor.submit(replicate_rpc, self.view, new_n, req)

                    # add callback
                    fut.add_done_callback(write_rpc_callback)

                    # add update information about new future in case this fails too: TODO: should add a lock
                    fut2replica[fut] = FutureInformation(req=req, hinted_handoff=new_n, original_node=future_information.original_node)

                else:
                    self.logger.info(f"[{self.identity(request)}] Writes done !")
                    replica_lock.acquire()
                    completed_reps += 1
                    replica_lock.release()

                    if completed_reps == self.params.N:
                        # this means our request is successfully replicated, we can RIP !
                        self.logger.info(f"[{self.identity(request)}] Put request has been successfully replicated :) ")

            return write_rpc_callback

        # get top N healthy nodes
        pref_list, pref_node_list = self.get_top_N_healthy_pref_list(token)
        callback = get_callback(executor, fut2replica, pref_list)

        self.logger.debug(f"[{self.identity(request)}] Tokens in pref_list are {pref_list}, and Nodes in pref_list are {pref_node_list}")
        for p in pref_node_list:
            # nessesary for storing in replica memory stores
            request.coord_nid = self.token2node[token]

            if p != self.n_id:
                # assuming no failures
                fut = executor.submit(replicate_rpc, self.view, p, request)
                # add description of future in a hash table
                fut_info = FutureInformation(req=request, original_node=p, hinted_handoff=-1)
                fut2replica[fut] = fut_info
                # add callback
                fut.add_done_callback(callback)
                fs.add(fut)
        # wait until max timeout, and check if W writes have succeeded, if yes then return else fail request
        itrs = concurrent.futures.as_completed(fs, timeout=self.params.w_timeout)

        failure = False
        if self.params.W > 1:
            try:
                w = 1 # already written on original node
                for it in itrs:  
                    if it.exception() is not None:
                        # this future did not work out, find alterate future node and put data there
                        self.logger.info(f"[{self.identity(request)}] Failure non callback !!")
                    else:
                        w += 1
                        self.logger.info(f"[{self.identity(request)}] ITRS: Writes to {w} nodes done !")
                        self.logger.info(f"[{self.identity(request)}] Replicated at {fut2replica[it].original_node}")
                    self.logger.info(f"[{self.identity(request)}] -----w is {w} and W is {self.params.W}-----")
                    if w >= self.params.W:
                        self.logger.info(f"[{self.identity(request)}] Breaking out of loop after satisfying min replicated nodes")
                        failure = False
                        break
            except concurrent.futures.TimeoutError:
                # time has expired
                failure = True
                self.logger.error(f"[{self.identity(request)}] Time has expired !")

        # fail if timeout or completed reps have not been done
        self.logger.info(f"[{self.identity(request)}] ----Completetd reps finally {completed_reps} Failure > {failure}, should we wait some more ?")
        
        num_wait_times = 0
        while completed_reps < self.params.W and not failure:
            if num_wait_times % 100 == 0:
                self.logger.info(f"[{self.identity(request)}] --------We are waiting....")
            time.sleep(0.001)
            num_wait_times += 1

            if num_wait_times > 1000:
                failure = True
                break

        if failure:
            return PutResponse(succ=False)

        # if we are here, we managed to replicate W nodes and the rest will be taken care of !
        return PutResponse(server_id=self.n_id, metadata="Replicated", reroute=False, reroute_server_id=-1, succ=True, context=request.context)
    

    # Debugging Functions
    def PrintMemory(self, request, context):
        """
        Prints current state of the node
        function meant for debugging purposes
        """
        self.logger.info("-------------------------------------------")
        self.logger.info(f"Information for tokens of node {self.n_id}")
        for token in self.membership_information[self.n_id]:
            self.logger.info(f"For token {token}, preference List: {get_preference_list_for_token(token, self.token2node, self.params)}")
        self.logger.info("The memory store for current node:")
        for key, val in self.memory_of_node.items():
            self.logger.info(f"Key: {key} | Val: {val.val}")
        
        self.logger.info("The memory store for replicated items:")
        for n_id, d in self.memory_of_replicas.items():
            self.logger.info(f"Replication for node {n_id}")
            for key, val in d.mem.items():
                self.logger.info(f"Key: {key} | Original Owner {find_owner(key, self.params, self.token2node)}| Val: {val.val} | Hinted Handoff {val.hinted_handoff}")
        
        self.logger.info("The membership information is:")
        for key, val in self.membership_information.items():
            ranges = get_ranges(val, self.params.Q)
            self.logger.info(f" Node {key} has the following tokens {ranges}")
        
        self.logger.info("-------------------------------------------")
        
        response = MemResponse(mem=self.memory_of_node, mem_replicated=self.memory_of_replicas)
        return response
    
    def Fail(self, request: FailRequest, context):
        """
        Fail this node, do not respond to future requests.
        """
        self.fail = request.fail
        self.logger.info(f"Node {self.n_id} is set to fail={self.fail}")
        return request
    
    def Gossip(self, request, context):
        """
        Turn gossip on for this node
        """
        self.start_gossip_protocol()
        return request

    def _check_add_latency(self):
        if self.network_params is None:
            return
        if self.network_params.randomize_latency:
            if self.network_params.distribution == 'uniform':
                latency = random.randint(0, 2*self.network_params.latency) / 1000
            else:
                latency = random.gauss(self.network_params.latency, self.network_params.latency) / 1000
                latency = max(0, latency)
            time.sleep(latency)
        else:
            time.sleep(self.network_params.latency / 1000)
