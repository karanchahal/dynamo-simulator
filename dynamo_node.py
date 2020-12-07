
from dynamo_pb2_grpc import DynamoInterfaceServicer
from typing import List, Tuple, Dict, Union
from dynamo_pb2 import PutResponse, GetResponse, PutRequest, GetRequest, ReplicateResponse, MemResponse, ReadResponse, ReadItem, Memory, VectorClockItem, FailRequest
from partitioning import get_preference_list, createtoken2node, find_owner, get_ranges
from structures import Params, NetworkParams, FutureInformation
from dynamo_pb2_grpc import DynamoInterfaceStub
import grpc
import threading
import concurrent
import random
import time

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
        self.big_pref_list = get_preference_list(n_id=n_id, membership_info=membership_information, params=params)
        self.preference_list = list(self.big_pref_list)[:self.params.N]

        self.token2node = createtoken2node(membership_information)

        # in memory data store of key, values
        self.memory_of_node: Dict[int, PutRequest] = {}
        self.memory_of_replicas: Dict[int, Memory] = {}

        # local view of failed nodes
        self.failed_node_lock = threading.Lock()
        self.failed_nodes = set({})

        # TODO: start gossip protocol


    def Get(self, request: GetRequest, context):
        """
        Get request
        """
        if self.fail:
            print(f"Node {self.n_id} is set to fail")
            raise NotImplementedError # retirning None will result in failure

        print(f"[GET] Get called by client {request.client_id} for key: {request.key}")
        self._check_add_latency()
        response: GetResponse = self._get_from_memory(request)
        return response

    def Read(self, request: GetRequest, context):
        """
        Read request

        Assumes current node has key
        """
        if self.fail:
            print(f"Node {self.n_id} is set to fail")
            raise NotImplementedError # retirning None will result in failure

        print(f"[Read] Read called for key {request.key} at node {self.n_id} at port {self.view[self.n_id]}")
        self._check_add_latency()
        response: ReadResponse = self._get_from_hash_table(request.key, coord_nid=request.coord_nid, from_replica=True)
        return response

    def Put(self, request: PutRequest , context):
        """
        Put Request
        """
        if self.fail:
            print(f"Node {self.n_id} is set to fail")
            raise NotImplementedError # retirning None will result in failure

        print(f"[Put] Put called for key {request.key} at node {self.n_id}")
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
            metadata="success")

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

        Returns a GetResponse or a PutResponse
        """
        # get coordinator node
        # search for closest node
        # walk up clockwise to find latest node

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
            raise NotImplementedError

    def read(self, request):
        """
        Read logic for dynamo. Assumes current node is the coordinator node.
        The request to replicate will be made asynchronously and after R replicas have 
        returned successfully, then we can return to client with success.

        Else this function will block until replication is done.
        TODO: add async operation
        """
        # print(f"Preference List is {self.preference_list}")
        
        # Stores result from all reads to check divergences
        items = []

        # Read data on current node
        item = self._get_from_hash_table(request.key).item
        # print(f'read found item with val: {item.val}')
        items.append(item)

        def rpc_callback(f):
            item = f.result().item
            print(f"read done, returning item with val {item.val} and clock {item.context.clock}")
            items.append(item)

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        fs = set([])
        for i, p in enumerate(self.preference_list):
            # nessasary for storing in replica memory stores
            request.coord_nid = self.n_id

            if i > self.params.N - 1:
                break
            if p != self.n_id:
                # assuming no failures
                fut = executor.submit(read_rpc, self.view, p, request)
                fut.add_done_callback(rpc_callback)
                fs.add(fut)
                # assume no failures: TODO: fix

        # wait until max timeout, and check if W writes have succeeded, if yes then return else fail request
        itrs = concurrent.futures.as_completed(fs, timeout=self.params.r_timeout)
        
        try:
            r = 1 # already written on original node
            for it in itrs:
                r += 1
                print(f"ITRS: Reads from {r} nodes done !")
                if r >= self.params.R:
                    break
            # TODO: store the futures that have not finished

        except concurrent.futures.TimeoutError:
            # time has expired
            print("Time has expired !")
            return GetResponse(server_id=self.n_id, items=items, metadata="timeout", reroute=False, reroute_server_id=-1)

        filtered_items = self._filter_latest(items)
        response = GetResponse(server_id=self.n_id, items=filtered_items, metadata="success", reroute=False, reroute_server_id=-1)
        return response


    def update_failed_nodes(self, node: int):
        """
        Update unhealthyness of a node, (will be reversed by gossip protocol)
        """
        self.failed_node_lock.aquire()
        self.failed_nodes.add(node)
        self.failed_node_lock.release()
    
    def get_spare_node(self, token_fail, req):
        """
        Move up the list and get a node that has not been used by the current request.
        This node will be used in the hinted handoff
        TODO: fix
        """
        # check big list for next node not current being used.
        # need to be careful about what kind of nodes have already been used.
        raise NotImplementedError
        tokens = self.tokens_used[req]

        last_token_used = tokens[-1]

        # move up clockwise
        while True:
            new_node = (last_node_used + 1) % self.params.num_proc
            # if this new node is not being used in this request, then use it.

        return new_node

    def get_top_N_healthy_pref_list(self):
        raise NotImplementedError

    def replicate(self, request):
        """
        Replication logic for dynamo DB. Assumes current node is the coordinator node.
        The request to replicate will be made asynchronously and after R replicas have 
        returned successfully, then we can return to client with success.

        Else this function will block until replication is done.
        """
        print(f"Preference List is {self.preference_list}")
        replica_lock = threading.Lock()
        completed_reps = 0
        fut2replica = {}
        failed_nodes = {}
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        fs = set([])

        def rpc_callback(f):
            """
            If future succeeds great !
            If future does not succeed, we have to
                1. find a spare node, send the request there with hinted handoff.
                2. mark node as unhealthy, gossip will mark it as healthy later on.
                3. once all N request have been replicated, we can rest in peace.
                4. store a global variable in higher order function, inc it until N
            """
            if f.exception():
                print(f"Future Failed !!")

                # get all information pertinent to future
                future_information = fut2replica[f]

                # add hinted handoff information
                req = future_information.req
                req.hinted_handoff = future_information.original_node # TODO: add hinted handoff info in put requests

                # TODO: update health of node
                failed_node_to_add = future_information.hinted_handoff if future_information.hinted_handoff else future_information.original_node 
                self.update_failed_nodes(failed_node_to_add)

                # TODO: implement this, get new candidate node from spare list
                new_n = self.get_spare_node()

                # make the call, add the current function so that it's called again
                fut = executor.submit(replicate_rpc, self.view, new_n, request)

                # add update information about new future in case this fails too
                fut2replica[fut] = FutureInformation(req=req, hinted_handoff=new_n, original_node=future_information.original_node)

            else:
                print(f"Writes done !")
                # TODO:
                # replica_lock.acquire()
                # completed_reps += 1

                # if completed_reps == N:
                #     # this means our request is successfully replicated, we can RIP !
                #     print("Put request has been successfully replicated :) ")
                # replica_lock.release()


        # send RPC's in parallel
        # TODO: use preference list to figure out top N healthy nodes

        # TODO: implement this
        # pref_list = self.get_top_N_healthy_pref_list()

        for p in self.preference_list:
            request.coord_nid = self.n_id
            if p != self.n_id:
                # assuming no failures
                fut = executor.submit(replicate_rpc, self.view, p, request)
                # add description of future in a hash table

                fut2replica[fut] = FutureInformation(req=request, original_node=p, hinted_handoff=None)

                fut.add_done_callback(rpc_callback)
                fs.add(fut)
                # assume no failures: TODO: fix

        # wait until max timeout, and check if W writes have succeeded, if yes then return else fail request
        itrs = concurrent.futures.as_completed(fs, timeout=self.params.w_timeout)
        failure = False
        try:
            w = 0 # already written on original node
            for it in itrs:  
                if it.exception() is not None:
                    # this future did not work out, find alterate future node and put data there
                    print("Failure !!")
                else:
                    w += 1
                    print(f"ITRS: Writes to {w} nodes done !")

                if w >= self.params.W:
                    print("Breaking out of loop after satisfying min replicated nodes")
                    break
            # TODO: store the futures that have not finished

        except concurrent.futures.TimeoutError:
            # time has expired
            failure = True
            print("Time has expired !")
            # TODO: fail request

        # fail if timeout or completed reps have not been done
        replica_lock.acquire()
        fail = failure or completed_reps >= self.params.W
        replica_lock.release()

        if fail:
            return PutResponse(succ=False)

        # if we are here, we managed to replicate W nodes and the rest will be taken care of !
        return PutResponse(server_id=self.n_id, metadata="Replicated", reroute=False, reroute_server_id=-1)
    

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
                print(f"Key: {key} | Original Owner {find_owner(key, self.params, self.token2node)}| Val: {val.val}")
        
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
