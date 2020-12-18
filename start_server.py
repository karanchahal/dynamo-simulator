from spawn import init_server
from structures import Params, NetworkParams

params = Params({
    'num_proc' : 8,
    'hash_size': 8, # 2^3 = 8 
    'Q' : 16, # 
    'N' : 4,
    'w_timeout': 2,
    'r_timeout': 2,
    'R': 2,
    'W': 3,
    'gossip': True,
    'update_failure_on_rpcs': False,
    'gossip_update_time': (0.5, 0.8)
})

network_params = NetworkParams({
    'latency': 50,
    'randomize_latency': True,
    'distribution': 'uniform',
    'drop_prob': 0
})

init_server(params, network_params)