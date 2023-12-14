from flask import Flask, request, Response

import jwt
import os
import json

app = Flask(__name__)

users = {}


def password_hasher(password):
    # TODO
    return password


def read_file_data(filename):
    with open(filename, 'rb') as file:
        return file.read()

@app.post('/signup')
def signup():
    data = json.loads(request.data)

    username = data.get('username', None)
    password = data.get('password', None)

    if username in users.keys():
        return Response(status=403)

    users[username] = password_hasher(password)

    private_key = read_file_data(
        os.environ.get('JWT_PRIVATE_KEY_FILE', 'signature.pem')
    )

    cookie = jwt.encode({'username': username}, private_key, 'RS256')

    response = Response(status=200)
    response.set_cookie('jwt', cookie)

    return response


@app.post('/login')
def login():
    data = json.loads(request.data)

    username = data.get('username', None)
    password = data.get('password', None)

    if username not in users.keys():
        return Response(status=403)

    if users[username] != password_hasher(password):
        return Response(status=403)

    private_key = read_file_data(
        os.environ.get('JWT_PRIVATE_KEY_FILE', 'signature.pem')
    )

    cookie = jwt.encode({'username': username}, private_key, 'RS256')

    response = Response(status=200)
    response.set_cookie('jwt', cookie)

    return response


if __name__ == '__main__':
    app.run(host="auth", port=8090)
