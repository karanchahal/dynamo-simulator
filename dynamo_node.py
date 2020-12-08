
from dynamo_pb2_grpc import DynamoInterfaceServicer
from typing import List, Tuple, Dict, Union
from dynamo_pb2 import PutResponse, GetResponse, PutRequest, GetRequest, ReplicateResponse, MemResponse, ReadResponse, ReadItem, Memory, VectorClockItem, FailRequest
from partitioning import createtoken2node, find_owner, get_ranges, get_preference_list_skip_unhealthy
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
        self.big_pref_list, _ = get_preference_list_skip_unhealthy(n_id=n_id, membership_info=membership_information, params=params)
        self.preference_list = list(self.big_pref_list)[:self.params.N]

        self.token2node = createtoken2node(membership_information)

        # in memory data store of key, values
        self.memory_of_node: Dict[int, PutRequest] = {}
        self.memory_of_replicas: Dict[int, Memory] = {}

        # local view of failed nodes
        self.failed_node_lock = threading.Lock()
        self.failed_nodes = set({})

        # keep track of all tokems used by a request
        self.tokens_used: Dict[int, List[int]] = {}
        


    def Get(self, request: GetRequest, context):
        """
        Get request
        """
        print(f"[GET] Get called by client {request.client_id} for key: {request.key} at node {self.n_id} at port {self.view[self.n_id]}")
        if self.fail:
            print(f"Node {self.n_id} at {self.view[self.n_id]} is set to fail, fail it !")
            raise concurrent.futures.CancelledError # retirning None will result in failure

        self._check_add_latency()
        response: GetResponse = self._get_from_memory(request)
        return response

    def Read(self, request: GetRequest, context):
        """
        Read request

        Assumes current node has key
        """
        print(f"[Read] Read called for key {request.key} at node {self.n_id} at port {self.view[self.n_id]}")
        if self.fail:
            print(f"Node {self.n_id} at {self.view[self.n_id]} is set to fail, fail it !")
            raise concurrent.futures.CancelledError # retirning None will result in failure
        try:
            self._check_add_latency()
            response: ReadResponse = self._get_from_hash_table(request.key, coord_nid=request.coord_nid, from_replica=True)
            return response
        except:
            print(f"----Exception {request.key} Nid: {self.n_id}")
            print(f"--------------------Exception raised ! {sys.exc_info()[0]}")
            response: ReadResponse = ReadResponse(succ=False)
        return response

    def Put(self, request: PutRequest , context):
        """
        Put Request
        """
        print(f"[Put] Put called for key {request.key} at node {self.n_id}")
        if self.fail:
            print(f"Node {self.n_id} is set to fail")
            raise concurrent.futures.CancelledError # retirning None will result in failure

        self._check_add_latency()
        # add to memory
        response: PutResponse = self._add_to_memory(request, request_type="put")

        print(f"Put sending a response back {response}")
        return response
    
    def Replicate(self, request: PutRequest , context):
        """
        Replicate Request
        """
        if self.fail:
            print(f"Node {self.n_id} is set to fail, fail it !")
            raise concurrent.futures.CancelledError # retirning None will result in failure

        print(f"Replication called for {request.key} at node {self.n_id}")
        self._check_add_latency()
        # add to memory
        self._add_to_replica_hash_table(request)

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
        key = request.key

        # find token for key
        req_token = key // self.params.Q

        # find node for token
        node = self.token2node[req_token]

        # if curr node is not the coordinator
        if self.n_id != node:
            # this request needs to be rerouted to first node in nodes
            return self.reroute(node, "get")

        # if curr node is coordinator node
        response = self.read(request)

        return response

    def _get_from_hash_table(self, key: int, coord_nid:int = None, from_replica: bool=False):
        """
        Used by the get/read requests and add it to the hash table
        """
        if from_replica:
            memory = self.memory_of_replicas[coord_nid].mem
        else:
            memory = self.memory_of_node
        put_request = memory[key] # currently we're storing the entire PutRequest in the hash table
        return ReadResponse(server_id=self.n_id,
            item=ReadItem(val=put_request.val, context=put_request.context),
            metadata="success", succ=True)

    def _add_to_hash_table(self, request):
        """
        Adds request to in memory hash table.
        """
        self.memory_of_node[request.key] = request
    
    def _add_to_replica_hash_table(self, request):
        """
        Adds request to in memory replicated hash table.
        """
        if request.coord_nid not in self.memory_of_replicas:
            self.memory_of_replicas[request.coord_nid] = Memory(mem={
                request.key: request
            })
        else:
            # print(f"---------- Adding k={request.key}, v={request.val}, clock={request.context.clock} to existing replica for node")
            mem_dict = dict(self.memory_of_replicas[request.coord_nid].mem)
            mem_dict[request.key] = request
            self.memory_of_replicas[request.coord_nid] = Memory(mem=mem_dict)

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


        # if curr node is not the coordinator
        if self.n_id != node:
            # this request needs to be rerouted to first node in nodes
            return self.reroute(node, request_type)

        # if curr node is coordinator node...
        # update context
        self._update_clock(request.context.clock)

        # store it
        self._add_to_hash_table(request)

        # send request to all replica nodes
        print(f"Replicating.... key={request.key}, val={request.val}")
        response = self.replicate(request)
        print(f"Replication done ! {self.memory_of_replicas}")

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
        print(f'Rerouting to {self.view[node]}...')
        if request_type == "put":
            return PutResponse(server_id=self.n_id, metadata="needs to reroute !", reroute=True, reroute_server_id=self.view[node])
        elif request_type == "get":
            return GetResponse(server_id=self.n_id, items=None, metadata="needs to reroute!", reroute=True, reroute_server_id=self.view[node])
        else:
            print("------------------This will always ERROR OUT")
            raise NotImplementedError

    def read(self, request):
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
        failed_nodes = {}
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        fs = set([])

        # Read data on current node
        item = self._get_from_hash_table(request.key).item
        items_lock = threading.Lock()
        # print(f'read found item with val: {item.val}')
        items.append(item)
        def get_callback(executor, fut2replica, token_used):
            nonlocal completed_reps
            nonlocal items
            def read_rpc_callback(f):
                nonlocal executor
                nonlocal fut2replica
                nonlocal token_used
                nonlocal completed_reps

                if self.read_failure(f):
                    # get all information pertinent to future
                    future_information = fut2replica[f]

                    # hinted handoff in get/read
                    req = future_information.req
                    # print(f"-------Future info of READ req {future_information}")
                    req.hinted_handoff = future_information.hinted_handoff if future_information.hinted_handoff != -1 else future_information.original_node

                    # update failed nodes, fo gossip to overturn in the future
                    failed_node_to_add = future_information.original_node
                    self.update_failed_nodes(failed_node_to_add)

                    # get new candidate node from spare list
                    new_n, new_token = self.get_spare_node(failed_node_to_add, token_used, req)
                    
                    # update used tokens memory
                    token_used.append(new_token)

                    print(f"New node selected for READ hinted handoff is {new_n}, firing request")

                    fut = executor.submit(read_rpc, self.view, new_n, req)

                    # add callback
                    fut.add_done_callback(read_rpc_callback)

                    fut2replica[fut] = FutureInformation(req=req, hinted_handoff=new_n, original_node=future_information.original_node)

                    
                else:
                    replica_lock.acquire()
                    completed_reps += 1
                    replica_lock.release()
                    items_lock.acquire()
                    item = f.result().item
                    items.append(item)
                    items_lock.release()
                    print(f"[READ callback] Successful {completed_reps} / {self.params.R}")

                    if completed_reps == self.params.N:
                        # this means our request is successfully replicated, we can RIP !
                        print("Get request has been successfully replicated :) ")

            return read_rpc_callback

        pref_list, tokens_used = self.get_top_N_healthy_pref_list()
        callback = get_callback(executor, fut2replica, tokens_used)
        print(f"[Coordinate READ]Pref list at {self.view[self.n_id]} is {pref_list}")
        for p in pref_list:
            # nessesary for storing in replica memory stores
            request.coord_nid = self.n_id

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
                    r += 1
                    print(f"ITRS: Reads from {r} out of {self.params.R} nodes done !")
                    if r >= self.params.R:
                        print(f"[Read coordinator] Breaking out of loop as {r} / {self.params.R} reads finishes")
                        break
                # TODO: store the futures that have not finished
            except concurrent.futures.TimeoutError:
                # time has expired
                failure = True
                print("[Read coordinator] Time has expired !")
                return GetResponse(server_id=self.n_id, items=items, metadata="timeout", reroute=False, reroute_server_id=-1)

        # TODO: add check for timeout too
        while completed_reps < self.params.R and not failure:
            print(f"--------We are waiting for read request to pass.... {completed_reps} {self.params.R}")
            time.sleep(0.001)
        
        if failure:
            # TODO: implement succ or failure of put response
            return GetResponse(server_id=self.n_id, succ=False)

        items_lock.acquire()
        filtered_items = self._filter_latest(items)
        items_lock.release()

        response = GetResponse(server_id=self.n_id, items=filtered_items, metadata="success", reroute=False, reroute_server_id=-1, succ=True)
        return response


    def update_failed_nodes(self, node: int):
        """
        Update unhealthyness of a node, (will be reversed by gossip protocol)
        """
        self.failed_node_lock.acquire()
        self.failed_nodes.add(node)
        self.failed_node_lock.release()
    
    def get_spare_node(self, token_fail, tokens_used: List[int],  req: PutRequest):
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
            if new_node not in nodes_used:
                token_used = new_token
                break
            # if this new node is not being used in this request, then use it.

        return new_node, token_used

    def get_top_N_healthy_pref_list(self):
        """
        If node is unhealthy in ring, then go ahead in ring until we get to healthy node
        """
        # print("In this")
        self.failed_node_lock.acquire()
        pref_list, token_list = get_preference_list_skip_unhealthy(n_id=self.n_id, membership_info=self.membership_information, params=self.params, unhealthy_nodes=self.failed_nodes)
        self.failed_node_lock.release()
        return pref_list, token_list
    
    def read_failure(self, fut):
        ret = fut.exception() is None
        if not fut.exception():
            res = fut.result()
            if res.succ:
                return False
        
        return True


    def replicate(self, request):
        """
        Replication logic for dynamo DB. Assumes current node is the coordinator node.
        The request to replicate will be made asynchronously and after R replicas have 
        returned successfully, then we can return to client with success.

        Else this function will block until replication is done.
        """
        replica_lock = threading.Lock()
        completed_reps = 1
        fut2replica = {}
        failed_nodes = {}
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        fs = set([])

        def get_callback(executor, fut2replica, failed_node, token_used):
            # print(f"Completed_reps !! {completed_reps}")
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
                nonlocal failed_nodes
                nonlocal completed_reps
                nonlocal token_used
                if f.exception():
                    print(f"Future Failed !!")

                    # get all information pertinent to future
                    future_information = fut2replica[f]

                    # add hinted handoff information
                    req = future_information.req
                    req.hinted_handoff = future_information.original_node

                    print(f"Hinted handoff request is {req}")

                    # TODO: update health of node
                    failed_node_to_add = future_information.hinted_handoff if future_information.hinted_handoff != -1 else future_information.original_node 

                    print(f"Failed node is {failed_node_to_add}")
                    self.update_failed_nodes(failed_node_to_add)

                    # get new candidate node from spare list
                    new_n, new_token = self.get_spare_node(failed_node_to_add, token_used, req)
                    
                    # update used tokens memory
                    token_used.append(new_token)

                    print(f"New node selected for hinted handoff is {new_n}")

                    # make the call, add the current function so that it's called again
                    fut = executor.submit(replicate_rpc, self.view, new_n, req)

                    # add callback
                    fut.add_done_callback(write_rpc_callback)

                    # add update information about new future in case this fails too
                    fut2replica[fut] = FutureInformation(req=req, hinted_handoff=new_n, original_node=future_information.original_node)

                else:
                    print(f"Writes done !")
                    replica_lock.acquire()
                    completed_reps += 1

                    if completed_reps == self.params.N:
                        # this means our request is successfully replicated, we can RIP !
                        print("Put request has been successfully replicated :) ")

                    replica_lock.release()

            return write_rpc_callback

        # get top N healthy nodes
        pref_list, token_used = self.get_top_N_healthy_pref_list()

        callback = get_callback(executor, fut2replica, failed_nodes, token_used)
        
        print(f"Pref List is  {pref_list}")
        for p in pref_list:
            # nessesary for storing in replica memory stores
            request.coord_nid = self.n_id

            if p != self.n_id:
                # assuming no failures
                fut = executor.submit(replicate_rpc, self.view, p, request)
                # add description of future in a hash table
                fut_info = FutureInformation(req=request, original_node=p, hinted_handoff=None)
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
                        print("Failure non callback !!")
                    else:
                        w += 1
                        print(f"ITRS: Writes to {w} nodes done !")
                        print(f"Replicated at {fut2replica[it].original_node}")
                    print(f"-----w is {w} and W is {self.params.W}-----")
                    if w >= self.params.W:
                        print("Breaking out of loop after satisfying min replicated nodes")
                        failure = False
                        break
                # TODO: store the futures that have not finished
            except concurrent.futures.TimeoutError:
                # time has expired
                failure = True
                print("Time has expired !")
                # TODO: fail request

        # fail if timeout or completed reps have not been done
        print(f"----Completetd reps finally {completed_reps} Failure > {failure}, should we wait some more ?")
        
        # TODO: add check for timeout too
        while completed_reps < self.params.W and not failure:
            print(f"--------We are waiting....")
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
        print("-------------------------------------------")
        print(f"Information for {self.n_id}")
        print(f"Preference List: {self.preference_list}")
        print("The memory store for current node:")
        for key, val in self.memory_of_node.items():
            print(f"Key: {key} | Val: {val.val}")
        
        print("The memory store for replicated items:")
        for n_id, d in self.memory_of_replicas.items():
            print(f"Replication for node {n_id}")
            for key, val in d.mem.items():
                print(f"Key: {key} | Original Owner {find_owner(key, self.params, self.token2node)}| Val: {val.val} | Hinted Handoff {val.hinted_handoff}")
        
        print("The membership information is:")
        for key, val in self.membership_information.items():
            ranges = get_ranges(val, self.params.Q)
            print(f" Node {key} has the following tokens {ranges}")
        
        print("-------------------------------------------")
        
        response = MemResponse(mem=self.memory_of_node, mem_replicated=self.memory_of_replicas)
        return response
    
    def Fail(self, request: FailRequest, context):
        """
        Fail this node, do not respond to future requests.
        """
        self.fail = request.fail
        print(f"Node {self.n_id} is set to fail={self.fail}")
        return request

    def _check_add_latency(self):
        if self.network_params is None:
            return
        if self.network_params.randomize_latency:
            time.sleep(random.randint(0, self.network_params.latency) / 1000)
        else:
            time.sleep(self.network_params.latency / 1000)
