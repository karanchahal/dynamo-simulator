from tqdm import tqdm
import numpy as np
import sys
sys.path.append('../')
from partitioning import is_pow_of_two, init_membership_list
from spawn import start_db_background
from structures import Params, NetworkParams
from client_dynamo import client_get, client_put
from parallel_runner import run_parallel
from random import randint

import logging
logger = logging.getLogger('dynamo_node')
logger.setLevel(logging.ERROR)

START_PORT = 2333
CLIENT_ID = 1

def get_start_port(randomize=True):
    return START_PORT + randint(0, params.num_proc-1) * int(randomize)

def get_stats(durations):
    if len(durations) == 0:
        return {}
    durations = np.array(durations) # convert from seconds to ms
    mean = np.mean(durations)
    std = np.std(durations)
    nnth = np.percentile(durations, 99.9)
    return {'mean': mean, 'std': std, '99.9th': nnth}

def generate_plot(durations, label='', clear=True):
    if clear:
        plt.clf()
    fig = sns.distplot(durations, label=label)
    plt.ylabel('Density')
    plt.xlabel('Response Time (in ms)')
    plt.title('Distribution of response times (in ms)')
    plt.legend()
    plt.show()

params = {
    'num_proc' : 8,
    'hash_size': 8, # 2^3 = 8 
    'Q' : 16, # 
    'N' : 4,
    'w_timeout': 2,
    'r_timeout': 2,
    'R': 1,
    'W': 3,
    'gossip': False
}
network_params = {
    'latency': 10,
    'randomize_latency': False,
    'drop_prob': 0
}

params = Params(params)
network_params = NetworkParams(network_params)
membership_information = init_membership_list(params)

processes_future = start_db_background(params, membership_information, network_params, wait=True, start_port=START_PORT)

def run_multiple_get(total, num_requests, get_durations):
    for i in tqdm(range(total // num_requests)):
        requests = [client_get]*num_requests
        requests_params = [{'port': get_start_port(), 'client_id': CLIENT_ID, 'key': randint(0, 2**params.hash_size-1)} for _ in range(num_requests)]
        get_durations = np.concatenate((get_durations, run_parallel(requests, requests_params, start_port=START_PORT)))
    return get_durations

def run_multiple_put(total, num_requests, put_durations):
    for i in tqdm(range(total // num_requests)):
        requests = [client_put]*num_requests
        k = randint(0, 2**params.hash_size-1)
        requests_params = [{'port': get_start_port(), 'client_id': CLIENT_ID, 'key': k, 'val': str(k)} for _ in range(num_requests)]
        put_durations = np.concatenate((put_durations, run_parallel(requests, requests_params, start_port=START_PORT)))
    return put_durations

def store_keys(params):
    for key in tqdm(range(2**params.hash_size)):
        client_put(get_start_port(), CLIENT_ID, key=key, val=str(key))

store_keys(params)

import logging
logger = logging.getLogger('dynamo_node')
logger.setLevel(logging.INFO)
logger.propagate = False

def get_start_port(randomize=True, failed_port=None):
    new_port = START_PORT + randint(0, params.num_proc-1) * int(randomize)
    while failed_port is not None and new_port == failed_port:
        new_port = START_PORT + randint(0, params.num_proc-1) * int(randomize)
    return new_port

def run_multiple_get(total, num_requests, get_durations=None, failed_port=None):
    durations, responses = [], []
    for i in tqdm(range(total // num_requests)):
        requests = [client_get]*num_requests
        requests_params = [{'port': get_start_port(failed_port=failed_port), 'client_id': CLIENT_ID, 'key': randint(0, 2**params.hash_size-1)} for _ in range(num_requests)]
        _durations, _responses = run_parallel(requests, requests_params, start_port=START_PORT, as_np=False)
        print(len(_durations))
        durations.extend(_durations)
        responses.extend(_responses)
    return durations, responses

from client_dynamo import client_fail

client_fail(START_PORT)

response = client_put(START_PORT+1, CLIENT_ID, 0, "0")
print(response)

response = client_get(START_PORT+1, CLIENT_ID, 0)
print(response)

get_fail_durations, responses = run_multiple_get(100, 10, None, START_PORT)

print(len(get_fail_durations))
print(get_fail_durations)
