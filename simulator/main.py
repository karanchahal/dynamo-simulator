from flask import Flask, render_template, redirect, url_for
from flask_bootstrap import Bootstrap
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, IntegerField, BooleanField, FormField, HiddenField
from wtforms.validators import DataRequired, NumberRange

import sys
sys.path.append('../')
from partitioning import is_pow_of_two
from spawn import start_db_background
from structures import Params, NetworkParams

from threading import Thread
import time

app = Flask(__name__)

# Flask-WTF requires an encryption key - the string can be anything
app.config['SECRET_KEY'] = 'dynamo'
# Disable CSRF for debugging
app.config['WTF_CSRF_ENABLED'] = False

# Flask-Bootstrap requires this line
Bootstrap(app)

class DynamoForm(FlaskForm):
    def validate_Q(form, field):
        return True
        #TODO:
        # return is_pow_of_two(field.data) and field.data <= 2**form.hash_size.data
    
    def validate_N(form, field):
        # TODO:
        return True

    name = StringField('Name of your configuration', default="Dynamo")
    num_proc = IntegerField('Number of nodes (1-10)', validators=[NumberRange(min=1, max=10)], default=4)
    hash_size = IntegerField('Number of bits in key space (1-20)', validators=[NumberRange(min=1, max=20)], default=3)
    Q = IntegerField('Number of tokens per node', validators=[], default=2)
    N = IntegerField('N', validators=[], default=2)
    R = IntegerField('R', default=1)
    W = IntegerField('W', default=1)
    w_timeout = IntegerField('Coordinator node write timeout (in seconds)', default=2)
    r_timeout = IntegerField('Coordinator node read timeout (in seconds)', default=2)

class NetworkForm(FlaskForm):
    randomize_latency = BooleanField('Randomize network latency?', default=False)
    latency = IntegerField('Max network latency (in ms)', default=0)

class CombinedForm(FlaskForm):
    dynamo_form = FormField(DynamoForm)
    network_form = FormField(NetworkForm)
    submit = SubmitField('Submit')

processes_future = None

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
        membership_information = {
            0: [1], # key space -> (2,4]
            1: [2], # key space -> (4,6]
            2: [3], # key space -> (6,8]
            3: [0] # key space -> (0,2]
        }
        network_params = NetworkParams({
            'latency': form.network_form.latency.data,
            'randomize_latency': form.network_form.randomize_latency.data
        })
        processes_future = start_db_background(params, membership_information, network_params, wait=True)
        print(processes_future.done())
    return render_template('index.html', form=form, message=message)

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
