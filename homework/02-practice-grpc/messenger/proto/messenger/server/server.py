from concurrent import futures
import threading
import logging
import os

import grpc
from messenger.proto import messenger_pb2
from messenger.proto import messenger_pb2_grpc
from google.protobuf.timestamp_pb2 import Timestamp


class Waiter:
    def __init__(self):
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.breaking = False

    def wait(self):
        while True:
            with self.lock:
                self.cv.wait()
                if self.breaking:  # double check to prevent spurious wakeups
                    self.breaking = False
                    return

    def notify(self):
        with self.lock:
            self.breaking = True
            self.cv.notify()


class MessengerServer(messenger_pb2_grpc.MessengerServerServicer):

    def __init__(self):
        self._waiters = dict()
        self._messages_lists = list()
        self._lock = threading.Lock()

    def Subscribe(self):
        with self._lock:
            index = len(self._waiters)
            self._waiters[index] = Waiter()
            self._messages_lists.append(list())
            return index

    def SendMessage(self, request, context):
        timestamp = Timestamp()
        timestamp.GetCurrentTime()

        with self._lock:
            message = {
                'author': request.author,
                'text': request.text,
                'sendTime': timestamp,
            }
            for i in range(len(self._waiters)):
                self._messages_lists[i].append(message)
                self._waiters[i].notify()

        return messenger_pb2.SendResponse(sendTime=timestamp)

    def ReadMessages(self, request, context):
        def response_messages():
            timestamp = Timestamp()
            timestamp.GetCurrentTime()

            subscription_index = self.Subscribe()

            while True:
                self._waiters[subscription_index].wait()

                with self._lock:
                    for message in self._messages_lists[subscription_index]:
                        yield messenger_pb2.MessageFromStreamRead(**message)
                    self._messages_lists[subscription_index].clear()

        return response_messages()


def serve():
    port = os.environ.get('MESSENGER_SERVER_PORT', '51075')

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    messenger_pb2_grpc.add_MessengerServerServicer_to_server(MessengerServer(), server)
    server.add_insecure_port("server:" + port)
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
