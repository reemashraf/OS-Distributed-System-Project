import flask
app = flask.Flask('your_flask_env')

@app.route('/yrab', methods=['GET', 'POST'])
def register():
    if flask.request.method == 'POST':
        username = flask.request.values.get('firstname') # Your form's
        password = flask.request.values.get('lastname') # input names
        print(firstname+lastname)
    else:
        # You probably don't have args at this route with GET
        # method, but if you do, you can access them like so:
        yourarg = flask.request.args.get('argname')
        your_register_template_rendering(yourarg)