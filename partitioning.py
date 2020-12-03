
import numpy as np
from typing import Dict, List
from structures import Params

def is_pow_of_two(n: int) -> bool:
    """
    Helper function to hceck if num is a power of two.
    """
    print(f"{n} and {(n & (n-1) == 0) and n != 0}")
    return (n & (n-1) == 0) and n != 0


def init_membership_list(params: Params) -> Dict[int, List[int]]: # 2^{12}, 12 bit key hash
    """
    Follows strategy 3 in the Dynamo Paper.

    Returns a dictionary indexing the nodes with membership info
    """

    assert(is_pow_of_two(params.Q) and is_pow_of_two(params.num_proc))
    
    key_space = pow(2, params.hash_size)
    total_v_nodes = round(key_space / params.Q)
    v_nodes_per_proc = round(total_v_nodes / params.num_proc)

    v_nodes = np.arange(total_v_nodes)

    print(f"Configuration is as follows: Key space sizes {key_space}")
    print(f"Total V Nodes {total_v_nodes} | V Nodes Per Proc {v_nodes_per_proc}")
    # randomly allot `num_v_nodes` to each node
    np.random.shuffle(v_nodes)

    alloc_v_nodes = np.reshape(v_nodes, (params.num_proc, v_nodes_per_proc))
    membership_info = {}
    for i in range(params.num_proc):
        membership_info[i] = alloc_v_nodes[i].tolist()

    return membership_info


def createtoken2node(membership_info: Dict[int, List[int]]) -> Dict[int, int]:
    """
    Creates a dict that has key -> token number and value -> node id
    """
    token2node = {}
    for key, val in membership_info.items():
        for v in val:
            token2node[v] = key
    
    return token2node


def get_preference_list(n_id, membership_info: Dict[int,List[int]], params: Params, timeout=1):
    '''
    Returns a pref list of size N (`N` specified in params).

    We use Strategy 3 as our consistent hashing paradigm:
    Q -> size of a token
    T -> number of tokens that cover entire key space K
    S - > number of nodes

    For each node, to find it's replicas. We find it's tokens (M = T/S tokens).
    For each of these tokens, we look at N/M tokens ahead and find the nodes responsible for those tokens.


    Hence, we would have M*(N/M) total tokens to look at. We find out the nodes responsible for those tokens
    and those nodes form our preference list. 

    Please note, that we can have less than N number of nodes as multiple tokens might map to the same node.

    TODO: Add fix to make this never happen.

    To resolve for this, 
    n_id: node id for which to construct pref list
    membership_info: List of tokens given to each node
    params: Dynamo params
    timeout: 1 second
    '''
    # get positions of each server in the ring
    
    key_space = pow(2, params.hash_size)
    total_v_nodes = round(key_space / params.Q)
    v_nodes_per_proc = round(total_v_nodes / params.num_proc)
    max_token = total_v_nodes - 1
    token2node = createtoken2node(membership_info) # could have multiple nodes for a single token

    n_per_token = round(params.N / v_nodes_per_proc)
    pref_list = set([])
    for token in membership_info[n_id]:
        for i in range(1,n_per_token+1):
            replic_node = (token+i) % total_v_nodes
            n = token2node[replic_node]
            if n != n_id:
                pref_list.add(n)

    # Note: this will trigger if we do not have at least N unique replicas
    assert len(pref_list) >= params.N

    return pref_list
