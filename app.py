from flask import jsonify, Flask, render_template, request
from flask.views import View
from ops import Db

app = Flask(__name__)


@app.route('/', methods=['POST', 'GET'])
def homepage():

    return render_template('newtable.html')

@app.route('/create_table', methods=['POST', 'GET'])
def create_table():
    return '<h1>hello</h1>'



if __name__ == '__main__':
    app.run()