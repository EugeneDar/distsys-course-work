from flask import Flask, request, Response

import jwt
import os
import argparse
import hashlib
import json

app = Flask(__name__)

values = {}
users = {}


def read_file_data(filename):
    with open(filename, 'rb') as file:
        return file.read()

@app.post('/put')
def put():
    cookie = request.cookies.get('jwt', None)
    key = request.args.get('key')
    value = json.loads(request.data).get('value', None)

    if not cookie:
        return Response(status=401)

    public_key = read_file_data(
        os.environ.get('JWT_PUBLIC_KEY_FILE', public_path)
    )

    try:
        decoded = jwt.decode(cookie, public_key, 'RS256')
        username = decoded['username']
    except Exception as e:
        return Response(status=400)

    if key in values:
        if username != users[key]:
            return Response(status=403)

    values[key] = value
    users[key] = username

    return Response(status=200)


@app.get('/get')
def get():
    cookie = request.cookies.get('jwt', None)
    key = request.args.get('key')

    if not cookie:
        return Response(status=401)

    public_key = read_file_data(
        os.environ.get('JWT_PUBLIC_KEY_FILE', public_path)
    )

    try:
        decoded = jwt.decode(cookie, public_key, 'RS256')
        username = decoded['username']
    except Exception as e:
        return Response(status=400)

    if key not in values:
        return Response(status=404)

    if username != users[key]:
        return Response(status=403)

    return Response(
        json.dumps({"value": values[key]}),
        status=200
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='auth')
    parser.add_argument('--private', type=str, default='/tmp/signature.pem')
    parser.add_argument('--public', type=str, default='/tmp/signature.pub')
    parser.add_argument('--port', type=int, default=8090)

    args = parser.parse_args()
    private_path, public_path, port = args.private, args.public, args.port

    app.run(host="kv", port=port)
