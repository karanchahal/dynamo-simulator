from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, IntegerField, BooleanField, FormField, HiddenField
from wtforms.validators import DataRequired, NumberRange
from wtforms.widgets import html_params, HTMLString

class ButtonWidget(object):
    """
    Renders a multi-line text area.
    `rows` and `cols` ought to be passed as keyword args when rendering.
    """
    input_type = 'submit'

    html_params = staticmethod(html_params)

    def __call__(self, field, **kwargs):
        kwargs.setdefault('id', field.id)
        kwargs.setdefault('type', self.input_type)
        if 'value' not in kwargs:
            kwargs['value'] = field._value()

        return HTMLString('<button {params}>{label}</button>'.format(
            params=self.html_params(name=field.name, **kwargs),
            label=field.label.text)
        )

class ButtonField(StringField):
    widget = ButtonWidget()

class DynamoForm(FlaskForm):
    def validate_Q(form, field):
        return True
        #TODO:
        # return is_pow_of_two(field.data) and field.data <= 2**form.hash_size.data
    
    def validate_N(form, field):
        # TODO:
        return True

    server_name = StringField('Name of your configuration', default="DynamoDB")
    num_proc = IntegerField('Number of nodes (1-10)', validators=[NumberRange(min=1, max=10)], default=4)
    hash_size = IntegerField('Number of bits in key space (1-20)', validators=[NumberRange(min=1, max=20)], default=3)
    Q = IntegerField('Q', validators=[], default=2)
    N = IntegerField('N', validators=[], default=2)
    R = IntegerField('R', default=1)
    W = IntegerField('W', default=1)
    w_timeout = IntegerField('Coordinator node write timeout (in seconds)', default=2)
    r_timeout = IntegerField('Coordinator node read timeout (in seconds)', default=2)
    gossip = BooleanField('Add gossip protocol', default=False)

class NetworkForm(FlaskForm):
    randomize_latency = BooleanField('Randomize network latency?', default=False)
    latency = IntegerField('Max network latency (in ms)', default=10)

class CombinedForm(FlaskForm):
    dynamo_form = FormField(DynamoForm)
    network_form = FormField(NetworkForm)
    submit = SubmitField('Submit')

class ClientForm(FlaskForm):
    key = IntegerField('Key', default=1)
    val = StringField('Value', default=1)
    num_requests = IntegerField('Number of requests', default=1)
    get_button = SubmitField('GET')
    put_button = SubmitField('PUT')
    clear = BooleanField('Clear Plot', default=True)
