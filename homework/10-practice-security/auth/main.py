from flask import Flask, request, Response

import jwt
import os
import argparse
import hashlib
import json

app = Flask(__name__)

users = {}


def password_hasher(username, password):
    m = hashlib.md5()
    m.update((username + password).encode())
    return m.hexdigest()


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

    users[username] = password_hasher(username, password)

    private_key = read_file_data(
        os.environ.get('JWT_PRIVATE_KEY_FILE', private_path)
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

    if users[username] != password_hasher(username, password):
        return Response(status=403)

    private_key = read_file_data(
        os.environ.get('JWT_PRIVATE_KEY_FILE', private_path)
    )

    cookie = jwt.encode({'username': username}, private_key, 'RS256')

    response = Response(status=200)
    response.set_cookie('jwt', cookie)

    return response

@app.get('/whoami')
def whoami():
    cookie = request.cookies.get('jwt', None)

    if cookie is None:
        return Response(status=401)

    public_key = read_file_data(
        os.environ.get('JWT_PUBLIC_KEY_FILE', public_path)
    )

    try:
        decoded = jwt.decode(cookie, public_key, 'RS256')
        username = decoded['username']
    except Exception as e:
        return Response(status=400)

    if username not in users:
        return Response(status=400)

    response = Response(f"Hello, {username}", status=200)
    return response


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='auth')
    parser.add_argument('--private', type=str, default='/tmp/signature.pem')
    parser.add_argument('--public', type=str, default='/tmp/signature.pub')
    parser.add_argument('--port', type=int, default=8090)

    args = parser.parse_args()
    private_path, public_path, port = args.private, args.public, args.port

    app.run(host="auth", port=port)
