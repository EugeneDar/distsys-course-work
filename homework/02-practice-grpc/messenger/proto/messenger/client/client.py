import copy
import json
import os
import random
import logging
import threading
from http import HTTPStatus
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import List, Dict

import grpc
from google.protobuf.json_format import ParseDict, MessageToDict
from google.protobuf.empty_pb2 import Empty
from messenger.proto import messenger_pb2
from messenger.proto import messenger_pb2_grpc


class PostBox:
    def __init__(self):
        self._messages: List[Dict] = []
        self._lock = threading.Lock()

    def collect_messages(self) -> List[Dict]:
        with self._lock:
            messages = copy.deepcopy(self._messages)
            self._messages = []
        return messages

    def put_message(self, message: Dict):
        with self._lock:
            self._messages.append(message)


class MessageHandler(BaseHTTPRequestHandler):
    _stub = None
    _postbox: PostBox

    def _read_content(self):
        content_length = int(self.headers['Content-Length'])
        bytes_content = self.rfile.read(content_length)
        return bytes_content.decode('ascii')

    # noinspection PyPep8Naming
    def do_POST(self):
        if self.path == '/sendMessage':
            response = self._send_message(self._read_content())
        elif self.path == '/getAndFlushMessages':
            response = self._get_messages()
        else:
            self.send_error(HTTPStatus.NOT_IMPLEMENTED)
            self.end_headers()
            return

        response_bytes = json.dumps(response).encode('ascii')
        self.send_response(HTTPStatus.OK)
        self.send_header('Content-Length', str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)

    def _send_message(self, content: str) -> dict:
        json_request = json.loads(content)

        request = ParseDict(json_request, messenger_pb2.SendRequest())

        response = self._stub.SendMessage(request)

        response_data = MessageToDict(response)

        return response_data

    def _get_messages(self) -> List[dict]:
        return self._postbox.collect_messages()


def stream_consumer_foo(stub, postbox):
    for message in stub.ReadMessages(messenger_pb2.ReadRequest()):
        postbox.put_message(MessageToDict(message))


def main():
    grpc_server_address = os.environ.get('MESSENGER_SERVER_ADDR', 'client:51075')

    channel = grpc.insecure_channel(grpc_server_address)
    stub = messenger_pb2_grpc.MessengerServerStub(channel)

    # A list of messages obtained from the server-py but not yet requested by the user to be shown
    # (via the http's /getAndFlushMessages).
    postbox = PostBox()

    # It should fetch messages via the grpc client and store them in the postbox.

    stream_consumer = threading.Thread(target=stream_consumer_foo, args=(stub, postbox))
    stream_consumer.start()

    # Pass the stub and the postbox to the HTTP server.
    # Dirty, but this simple http server doesn't provide interface
    # for passing arguments to the handler c-tor.
    MessageHandler._stub = stub
    MessageHandler._postbox = postbox

    http_port = os.environ.get('MESSENGER_HTTP_PORT', '8080')
    http_server_address = ('0.0.0.0', int(http_port))

    # NB: handler_class is instantiated for every http request. Do not store any inter-request state in it.
    httpd = HTTPServer(http_server_address, MessageHandler)
    httpd.serve_forever()


if __name__ == '__main__':
    logging.basicConfig()
    logging.info('Client started')
    main()
