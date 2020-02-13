from flask import Flask, escape, request

app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def hello():
    if request.method == 'GET':
        return f'Service Online'
    if request.method == 'POST':
        data = request.get_json()
        print(data['price'])
        return f"Fiyat: {data['price']}"
