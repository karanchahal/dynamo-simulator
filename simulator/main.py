import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
plt.style.use('seaborn')
from random import randint
from flask import Flask, render_template, redirect, url_for, session
from flask.signals import message_flashed
from flask_bootstrap import Bootstrap
from typing import List, Dict
from forms import CombinedForm, ClientForm

import sys
sys.path.append('../')
from partitioning import is_pow_of_two, init_membership_list
from spawn import start_db_background
from structures import Params, NetworkParams
from client_dynamo import client_get, client_put
from parallel_runner import run_parallel

from threading import Thread
import time
import logging

app = Flask(__name__)

# Flask-WTF requires an encryption key - the string can be anything
app.config['SECRET_KEY'] = 'dynamo'
# Disable CSRF for debugging
app.config['WTF_CSRF_ENABLED'] = False

# Flask-Bootstrap requires this line
Bootstrap(app)

processes_future = None
context = None
START_PORT = 2333
CLIENT_ID = 1

def get_start_port(randomize=True):
    return START_PORT + randint(0, session['params']['num_proc']-1) * int(randomize)

def get_stats(durations) -> Dict[str, int]:
    if len(durations) == 0:
        return {}
    durations = np.array(durations) * 1000 # convert from seconds to ms
    mean = np.mean(durations)
    std = np.std(durations)
    nnth = np.percentile(durations, 99.9)
    return {'mean': mean, 'std': std, '99.9th': nnth}

def generate_plot(durations, label='', clear=True):
    if clear:
        plt.clf()
    fig = sns.distplot(durations*1000, label=label)
    plt.ylabel('Density')
    plt.xlabel('Response Time (in ms)')
    plt.title('Distribution of response times (in ms)')
    location = f'static/images/plot_{time.time()}.png'
    plt.legend()
    plt.savefig(location)
    url = f'/{location}'
    return url

@app.route('/', methods=['GET', 'POST'])
def index():
    global processes_future

    logging.basicConfig(filename='simulator.log', level=logging.INFO)
    logger = logging.getLogger('simulator.log')

    form = CombinedForm()
    message = ""
    if form.validate_on_submit():
        message = "Started Dynamo"
        params = Params({
            'num_proc': form.dynamo_form.num_proc.data,
            'hash_size': form.dynamo_form.hash_size.data,
            'Q': form.dynamo_form.Q.data,
            'N': form.dynamo_form.N.data,
            'R': form.dynamo_form.R.data,
            'W': form.dynamo_form.W.data,
            'w_timeout': form.dynamo_form.w_timeout.data,
            'r_timeout': form.dynamo_form.r_timeout.data,
            'gossip': form.dynamo_form.gossip.data,
            'update_failure_on_rpcs': False
        })

        print(params)
        membership_information = init_membership_list(params)
        network_params = NetworkParams({
            'latency': form.network_form.latency.data,
            'randomize_latency': form.network_form.randomize_latency.data
        })
        processes_future = start_db_background(params, membership_information, network_params, wait=True, start_port=START_PORT, logger=logger)
        session['server_name'] = form.dynamo_form.server_name.data
        session['params'] = params.__dict__
        session['network_params'] = network_params.__dict__
        session['membership_information'] = membership_information
        return redirect(url_for('client'))
    return render_template('index.html', form=form, message=message)

@app.route('/client', methods=['GET', 'POST'])
def client():
    global context
    form = ClientForm()
    response = ""
    duration = ""
    durations = []
    stats = ""
    url = ""
    if form.is_submitted():
        start = time.time()
        if form.get_button.data:
            # print("GET:", form.get_button.data)
            if form.num_requests.data == 1:
                response = client_get(get_start_port(), CLIENT_ID, key=form.key.data)
                context = response.items[0].context
                duration = str((time.time()-start) * 1000)
            else:
                requests = [client_get]*form.num_requests.data
                requests_params = [{'port': get_start_port(), 'client_id': CLIENT_ID, 'key': form.key.data} for _ in range(form.num_requests.data)]
                durations, responses = run_parallel(requests, requests_params, start_port=START_PORT)
                duration = sum(durations)*1000
                stats = str(get_stats(durations))
                url = generate_plot(durations, 'GET', form.clear.data)
        else:
            # print("PUT:", form.put_button.data)
            if form.num_requests.data == 1:
                response = client_put(get_start_port(), CLIENT_ID, key=form.key.data, val=form.val.data, context=context)
                duration = str((time.time()-start) * 1000)
            else:
                requests = [client_put]*form.num_requests.data
                requests_params = [{'port': get_start_port(), 'client_id': CLIENT_ID, 'key': form.key.data, 'val': form.val.data} for _ in range(form.num_requests.data)]
                durations, responses = run_parallel(requests, requests_params, start_port=START_PORT)
                duration = sum(durations)*1000
                stats = str(get_stats(durations))
                url = generate_plot(durations, 'PUT', form.clear.data)
    return render_template('client.html', server_name=session['server_name'], form=form, response=str(response), duration=duration, stats=stats, url=url)

def background_task():
    '''
    Periodically prints the status of the dynamo servers
    '''
    global processes_future
    while True:
        if processes_future is not None:
            print('Done:', processes_future.done())
        time.sleep(1)

if __name__ == '__main__':
    # t = Thread(target=background_task)
    # t.start()
    app.run(debug=True)
