
from dynamo_pb2_grpc import DynamoInterfaceServicer
from typing import List, Tuple, Dict
from dynamo_pb2 import PutResponse, GetResponse
from partioning import get_preference_list


class DynamoNode(DynamoInterfaceServicer):
    """
    This class is the entry point to a dynamo node.
    """
    def __init__(self, n_id: int = None, view: Dict[int, str] = None, positions : List[int] = None):
        super().__init__()

        self.n_id = n_id

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
        response = GetResponse(
            server_id=self.n_id,
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

        response = PutResponse(
            server_id=self.n_id,
            metadata="Success ?")
        return response
    
    def _add_to_memory(self, request):
        # get coordinator node
        # search for closest node
        # walk up clockwise to find latest node
        return
        raise NotImplementedError