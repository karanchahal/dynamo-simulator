# from simulator.forms import ClientForm
from flask import Flask, render_template, redirect, url_for, session
from flask.signals import message_flashed
from flask_bootstrap import Bootstrap
from forms import CombinedForm, ClientForm

import sys
sys.path.append('../')
from partitioning import is_pow_of_two, init_membership_list
from spawn import start_db_background
from structures import Params, NetworkParams
from client_dynamo import client_get, client_put

from threading import Thread
import time

app = Flask(__name__)

# Flask-WTF requires an encryption key - the string can be anything
app.config['SECRET_KEY'] = 'dynamo'
# Disable CSRF for debugging
app.config['WTF_CSRF_ENABLED'] = False

# Flask-Bootstrap requires this line
Bootstrap(app)

processes_future = None
START_PORT = 2333
CLIENT_ID = 1

@app.route('/', methods=['GET', 'POST'])
def index():
    global processes_future
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
            'r_timeout': form.dynamo_form.r_timeout.data
        })
        #TODO: Update this
        membership_information = init_membership_list(params)
        # membership_information = {
        #     0: [1], # key space -> (2,4]
        #     1: [2], # key space -> (4,6]
        #     2: [3], # key space -> (6,8]
        #     3: [0] # key space -> (0,2]
        # }
        network_params = NetworkParams({
            'latency': form.network_form.latency.data,
            'randomize_latency': form.network_form.randomize_latency.data
        })
        processes_future = start_db_background(params, membership_information, network_params, wait=True, start_port=START_PORT)
        # print(processes_future.done())
        # session['processes_future'] = processes_future
        session['server_name'] = form.dynamo_form.server_name.data
        session['params'] = params.__dict__
        session['network_params'] = network_params.__dict__
        session['membership_information'] = membership_information
        return redirect(url_for('client'))
    return render_template('index.html', form=form, message=message)

@app.route('/client', methods=['GET', 'POST'])
def client():
    form = ClientForm()
    response = ""
    context = None
    if form.is_submitted():
        if form.get_button.data:
            print("GET:", form.get_button.data)
            response = client_get(START_PORT, CLIENT_ID, key=form.key.data)
            context = response.items[0].context
        else:
            print("PUT:", form.put_button.data)
            response = client_put(START_PORT, CLIENT_ID, key=form.key.data, val=form.val.data, context=context)
        print("\n-------Response:", response)
    return render_template('client.html', server_name=session['server_name'], form=form, response=str(response))

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
