
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

logger = logging.getLogger('dynamo_node')
logger.setLevel(logging.INFO)

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
        network_params: NetworkParams):
        """
        view: dict indexed by node id, value is the address of the node
        membership_information: dict indexed by node id, gives set of tokens for each node. 
            Each token states the starting point of a virtual node which is of size `Q` (see `Params`).
        params: Stores config information of the dynamo ring
        """
        super().__init__()

        self.params = params
        self.network_params = network_params

        self.n_id = n_id

        # tokens given to various nodes in the ring: bootstrapped information
        self.membership_information = membership_information

        # list of address to other nodes, indexed by node id
        self.view = view

        # whether set node to fail
        self.fail = False

        # a list of > N nodes that are closest to current node, (clockwise)
        # self.big_pref_list, _ = get_preference_list_skip_unhealthy(n_id=n_id, membership_info=membership_information, params=params)
        # self.preference_list = list(self.big_pref_list)[:self.params.N]

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
        logger.info(f"--------Vals to send ? {vals_to_send}")
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        request = DataBunchRequest(sent_from=self.n_id, requests=vals_to_send)

        def catch_callback(fut):
            """
            If future returns successfully, then we can go ahead and remove the extra
            stuff from the local memory
            """
            nonlocal vals_to_send

            if fut.exception() or fut.result().succ == False:
                logger.error(f"Error recieved in [tranferring data], reverse health of the node {n_id}")
                self.update_failed_nodes(n_id)
            else:
                logger.info(f"Success in transferring data at node {n_id}, we can delete our data now")
                to_del = []
                self.memory_of_replicas_lock.acquire()
                for k, mem_of_n in self.memory_of_replicas.items():
                    # iterate over memory of all n's
                    for key, val in mem_of_n.mem.items():
                        if val in vals_to_send:
                            to_del.append((k, key))
                
                for k1,k2 in to_del:
                    del self.memory_of_replicas[k1].mem[k2]
                
                logger.info(f"Deletion of {n_id}'s hinted handoff information successfull")
                self.memory_of_replicas_lock.release()



        fut = executor.submit(transfer_data_rpc, self.view, n_id, request)
        fut.add_done_callback(catch_callback)
        

    
    def start_gossip_protocol(self):
        # start a thread in the background that pings random nodes to determine health
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        mem_lock = threading.Lock()
        fut2req = {}
        def get_heartbeat(fut):
            nonlocal mem_lock
            nonlocal fut2req

            if fut.exception():
                
                # heartbeat failed, add node to unhealthy
                mem_lock.acquire()
                bad_n_id = fut2req[fut]
                mem_lock.release()
                logger.error(f"Future at node {self.n_id} for node {bad_n_id} had exception {fut.exception()}") 
                self.update_failed_nodes(bad_n_id, remove=False)
            else:
                # all good with node, do nothing !
                res = fut.result()
                if res.succ != True:
                    # all bad
                    self.update_failed_nodes(res.sent_to, remove=False)
                else:
                    self.update_failed_nodes(res.sent_to, remove=True)
                    # do scan in replica memory and send it to responsible node
                    self.scan_and_send(res.sent_to)
            
            # delete fut from dict, to prevent memeory leak
            mem_lock.acquire()
            del fut2req[fut]
            mem_lock.release()


        def ping_random_node():
            nonlocal executor
            nonlocal mem_lock
            nonlocal fut2req

            while True:
                n_id = self.n_id
                while n_id == self.n_id:
                    n_id = random.randint(0,self.params.num_proc-1)
                
                request = HeartbeatRequest(sent_to=n_id, from_n=self.n_id)
                fut = executor.submit(heartbeat_rpc, self.view, n_id, request)

                fut.add_done_callback(get_heartbeat)

                mem_lock.acquire()
                fut2req[fut] = n_id
                mem_lock.release()

                # sleep for 2 seconds
                # time.sleep(0.2)

        fut = executor.submit(ping_random_node)

    def TransferData(self, request: DataBunchRequest, context):
        """
        Adds all data to it's replica buffer or it's memory buffer ?
        1. if data is being transferred, then assumption is that it will be transferred to replica mem
        2. If coordinator node goes down, then some other node should take it's place, with hinted handoff.

        For each data item, check if key makes it current nodes owner, if not, then store in replic memory
        
        TODO: add locks
        """
        logger.info(f"[TransferData] at node = {self.n_id} at port {self.view[self.n_id]}")
        self._check_add_latency()
        if self.fail:
            logger.error(f"Node {self.n_id} at {self.view[self.n_id]} is set to fail, fail it !")
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
            logger.error(f"Error with [TransferData] at node = {self.n_id} at port {self.view[self.n_id]}")
            return DataBunchResponse(sent_from=self.n_id, succ=False)
            
        


    def Get(self, request: GetRequest, context):
        """
        Get request
        """
        logger.info(f"[GET] called by client {request.client_id} for key: {request.key} at node {self.n_id} at port {self.view[self.n_id]}")
        self._check_add_latency()
        if self.fail:
            logger.info(f"Node {self.n_id} at {self.view[self.n_id]} is set to fail, fail it !")
            raise concurrent.futures.CancelledError # retirning None will result in failure

        response: GetResponse = self._get_from_memory(request)
        return response
    
    def Heartbeat(self, request: GetRequest, context):
        """
        Get request
        """
        logger.info(f"[Heartbeat] called by client {request.from_n} at node {self.n_id} at port {self.view[self.n_id]}")
        self._check_add_latency()
        if self.fail:
            logger.error(f"Node {self.n_id} at {self.view[self.n_id]} is set to fail, fail it !")
            raise concurrent.futures.CancelledError # retirning None will result in failure
        
        request.succ = True
        return request

    def Read(self, request: GetRequest, context):
        """
        Read request

        Assumes current node has key
        """
        logger.info(f"[Read] called for key {request.key} at node {self.n_id} at port {self.view[self.n_id]}")
        self._check_add_latency()
        if self.fail:
            logger.info(f"Node {self.n_id} at {self.view[self.n_id]} is set to fail, fail it !")
            raise concurrent.futures.CancelledError # retirning None will result in failure
        try:
            req_token = request.key // self.params.Q
            node = self.token2node[req_token]
            if self.n_id == node:
                response = self._get_from_hash_table(request.key)
            else:  
                response = self._get_from_hash_table(request.key, coord_nid=request.coord_nid, from_replica=True)
            return response
        except:
            logger.error(f"----Exception {request.key} Nid: {self.n_id}")
            logger.error(f"--------------------Exception raised ! {sys.exc_info()[0]}")
            response: ReadResponse = ReadResponse(succ=False)
        return response

    def Put(self, request: PutRequest , context):
        """
        Put Request
        """
        logger.info(f"[Put] called for key {request.key} at node {self.n_id} at port {self.view[self.n_id]}")
        self._check_add_latency()
        if self.fail:
            logger.info(f"Node {self.n_id} is set to fail")
            raise concurrent.futures.CancelledError # retirning None will result in failure

        # add to memory
        response: PutResponse = self._add_to_memory(request, request_type="put")

        logger.info(f"Put sending a response back {response}")
        return response
    
    def Replicate(self, request: PutRequest , context):
        """
        Replicate Request
        """
        logger.info(f"[Replicate] called for key {request.key} at node {self.n_id}")
        self._check_add_latency()

        if self.fail:
            logger.info(f"Node {self.n_id} is set to fail, fail it !")
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
        
=        
        return response

    def _get_from_memory(self, request: GetRequest):
        """
        Tries to read from the memory of the node if the key is in the range of the current node.
        If not, the request the rerouted to the appropriate node.

        Returns a GetResponse
        """
        key = request.key

        # find token for key
        req_token = key // self.params.Q

        # find node for token
        node = self.token2node[req_token]

        pref_list_for_token, node_list = self.get_top_N_healthy_pref_list(req_token)

        # if curr node is not the coordinator
        if self.n_id not in node_list:
            # this request needs to be rerouted to first node in nodes
            return self.reroute(node, "get")

        # if curr node is coordinator node
        response = self.read(request, req_token)

        return response

    def _get_from_hash_table(self, key: int, coord_nid:int = None, from_replica: bool=False):
        """
        Used by the get/read requests and add it to the hash table
        """
        if from_replica:
            self.memory_of_replicas_lock.acquire()
            memory = self.memory_of_replicas[coord_nid].mem
            put_request = memory[key]
            self.memory_of_replicas_lock.release()
        else:
            self.memory_of_node_lock.acquire()
            memory = self.memory_of_node
            put_request = memory[key]
            self.memory_of_node_lock.release()

        return ReadResponse(server_id=self.n_id,
            item=ReadItem(val=put_request.val, context=put_request.context),
            metadata="success", succ=True)

    def _add_to_hash_table(self, request, add_to_replica=False, coord_id=None):
        """
        Adds request to in memory hash table.
        """
        if not add_to_replica:
            self.memory_of_node_lock.acquire()
            self.memory_of_node[request.key] = request
            self.memory_of_node_lock.release()
        else:
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
        logger.info(f"Replicating.... key={request.key}, val={request.val}")
        response = self.replicate(request, req_token)
        logger.info(f"Replication done ! {self.memory_of_replicas}")

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
        logger.info(f'Rerouting to {self.view[node]}...')
        if request_type == "put":
            return PutResponse(server_id=self.n_id, metadata="needs to reroute !", reroute=True, reroute_server_id=self.view[node])
        elif request_type == "get":
            return GetResponse(server_id=self.n_id, items=None, metadata="needs to reroute!", reroute=True, reroute_server_id=self.view[node])
        else:
            logger.error("------------------This will always ERROR OUT")
            raise NotImplementedError

    def read(self, request, token):
        """
        Read logic for dynamo. Assumes current node is the coordinator node.
        The request to replicate will be made asynchronously and after R replicas have 
        returned successfully, then we can return to client with success.

        Else this function will block until replication is done.
        TODO: add async operation
        """
        
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

        if main_node != self.n_id:
            item = self._get_from_hash_table(request.key, coord_nid=main_node, from_replica=True).item
        else:
            item = self._get_from_hash_table(request.key).item

        items_lock = threading.Lock()
        items.append(item)

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
                    self.update_failed_nodes(failed_node_to_add)

                    # get new candidate node from spare list
                    new_n, new_token = self.get_spare_node(tokens_used)

                    # update used tokens memory
                    tokens_used.append(new_token)

                    logger.info(f"New node selected for READ hinted handoff is {new_n}, firing request")

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

                    logger.info(f"[READ callback] Successful {completed_reps} / {self.params.N}")

                    if completed_reps == self.params.N:
                        # this means our request is successfully replicated, we can RIP !
                        logger.info("Get request has been successfully replicated :) ")

            return read_rpc_callback

        pref_list, pref_node_list = self.get_top_N_healthy_pref_list(token)
        callback = get_callback(executor, fut2replica, pref_list)
        logger.info(f"[Coordinate READ] Pref list at {self.view[self.n_id]} is {pref_list}")
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
                        logger.info(f"ITRS: Reads from {r} out of {self.params.R} nodes done !")
                        if r >= self.params.R:
                            logger.info(f"[Read coordinator] Breaking out of loop as {r} / {self.params.R} reads finishes")
                            break
                # TODO: store the futures that have not finished
            except concurrent.futures.TimeoutError:
                # time has expired
                failure = True
                logger.error("[Read coordinator] Time has expired !")
                return GetResponse(server_id=self.n_id, items=items, metadata="timeout", reroute=False, reroute_server_id=-1)

        # TODO: add check for timeout too
        while completed_reps < self.params.R and not failure:
            logger.info(f"--------We are waiting for read request to pass.... {completed_reps} {self.params.R}")
            time.sleep(0.001)
        
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
        TODO: fix
        """
        # check big list for next node not current being used.
        # need to be careful about what kind of nodes have already been used.
        nodes_used = [self.token2node[t] for t in tokens_used]
        last_token_used = tokens_used[-1]

        # move up clockwise
        token_used = None
        while True:
            new_token = (last_token_used + 1) % self.params.num_proc
            new_node = self.token2node[new_token]
            self.failed_node_lock.acquire()
            if new_node not in nodes_used and new_node not in self.failed_nodes:
                self.failed_node_lock.release()
                token_used = new_token
                break
            self.failed_node_lock.release()

        return new_node, token_used

    def get_top_N_healthy_pref_list(self, token):
        """
        If node is unhealthy in ring, then go ahead in ring until we get to healthy node
        """
        self.failed_node_lock.acquire()
        # pref_list, token_list = get_preference_list_skip_unhealthy(n_id=self.n_id, membership_info=self.membership_information, params=self.params, unhealthy_nodes=self.failed_nodes)
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

                    logger.info(f"Hinted handoff request is {req}")

                    # TODO: update health of node
                    failed_node_to_add = future_information.hinted_handoff if future_information.hinted_handoff != -1 else future_information.original_node

                    logger.info(f"Failed node is {failed_node_to_add}")
                    self.update_failed_nodes(failed_node_to_add)

                    # get new candidate node from spare list
                    new_n, new_token = self.get_spare_node(tokens_used)

                    # update used tokens memory
                    tokens_used.append(new_token)

                    logger.info(f"New node selected for hinted handoff is {new_n}")

                    # make the call, add the current function so that it's called again
                    fut = executor.submit(replicate_rpc, self.view, new_n, req)

                    # add callback
                    fut.add_done_callback(write_rpc_callback)

                    # add update information about new future in case this fails too
                    fut2replica[fut] = FutureInformation(req=req, hinted_handoff=new_n, original_node=future_information.original_node)

                else:
                    logger.info(f"Writes done !")
                    replica_lock.acquire()
                    completed_reps += 1
                    replica_lock.release()

                    if completed_reps == self.params.N:
                        # this means our request is successfully replicated, we can RIP !
                        logger.info("Put request has been successfully replicated :) ")

            return write_rpc_callback

        # get top N healthy nodes
        pref_list, pref_node_list = self.get_top_N_healthy_pref_list(token)
        callback = get_callback(executor, fut2replica, pref_list)

        logger.info(f"Tokens in pref_list are {pref_list}, and Nodes in pref_list are {pref_node_list}")
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
                # assume no failures: TODO: fix
        # wait until max timeout, and check if W writes have succeeded, if yes then return else fail request
        itrs = concurrent.futures.as_completed(fs, timeout=self.params.w_timeout)

        failure = False
        if self.params.W > 1:
            try:
                w = 1 # already written on original node
                for it in itrs:  
                    if it.exception() is not None:
                        # this future did not work out, find alterate future node and put data there
                        logger.info("Failure non callback !!")
                    else:
                        w += 1
                        logger.info(f"ITRS: Writes to {w} nodes done !")
                        logger.info(f"Replicated at {fut2replica[it].original_node}")
                    logger.info(f"-----w is {w} and W is {self.params.W}-----")
                    if w >= self.params.W:
                        logger.info("Breaking out of loop after satisfying min replicated nodes")
                        failure = False
                        break
                # TODO: store the futures that have not finished
            except concurrent.futures.TimeoutError:
                # time has expired
                failure = True
                logger.info("Time has expired !")
                # TODO: fail request

        # fail if timeout or completed reps have not been done
        logger.info(f"----Completetd reps finally {completed_reps} Failure > {failure}, should we wait some more ?")
        
        # TODO: add check for timeout too
        while completed_reps < self.params.W and not failure:
            logger.info(f"--------We are waiting....")
            time.sleep(0.001)

        if failure:
            # TODO: implement succ or failure of put response
            return PutResponse(succ=False)

        # if we are here, we managed to replicate W nodes and the rest will be taken care of !
        return PutResponse(server_id=self.n_id, metadata="Replicated", reroute=False, reroute_server_id=-1, succ=True)
    

    # Debugging Functions
    def PrintMemory(self, request, context):
        """
        Prints current state of the node
        function meant for debugging purposes
        """
        logger.info("-------------------------------------------")
        logger.info(f"Information for tokens of node {self.n_id}")
        for token in self.membership_information[self.n_id]:
            logger.info(f"For token {token}, preference List: {get_preference_list_for_token(token, self.token2node, self.params)}")
        logger.info("The memory store for current node:")
        for key, val in self.memory_of_node.items():
            logger.info(f"Key: {key} | Val: {val.val}")
        
        logger.info("The memory store for replicated items:")
        for n_id, d in self.memory_of_replicas.items():
            logger.info(f"Replication for node {n_id}")
            for key, val in d.mem.items():
                logger.info(f"Key: {key} | Original Owner {find_owner(key, self.params, self.token2node)}| Val: {val.val} | Hinted Handoff {val.hinted_handoff}")
        
        logger.info("The membership information is:")
        for key, val in self.membership_information.items():
            ranges = get_ranges(val, self.params.Q)
            logger.info(f" Node {key} has the following tokens {ranges}")
        
        logger.info("-------------------------------------------")
        
        response = MemResponse(mem=self.memory_of_node, mem_replicated=self.memory_of_replicas)
        return response
    
    def Fail(self, request: FailRequest, context):
        """
        Fail this node, do not respond to future requests.
        """
        self.fail = request.fail
        logger.info(f"Node {self.n_id} is set to fail={self.fail}")
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
            time.sleep(random.randint(0, self.network_params.latency) / 1000)
        else:
            time.sleep(self.network_params.latency / 1000)
