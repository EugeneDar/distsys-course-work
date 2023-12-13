from dslabmp import Context, Message, Process
from typing import List


class StorageNode(Process):
    def __init__(self, node_id: str, nodes: List[str]):
        self._id = node_id
        self._nodes = set(nodes)
        self._data = {}

    def _find_key_owner(self, key):
        init_id = list(self._nodes)[0]

        max_hash = hash(f'{init_id}{key}')
        max_id = init_id

        for node_id in self._nodes:
            current_hash = hash(f'{node_id}{key}')
            if current_hash > max_hash:
                max_hash = current_hash
                max_id = node_id

        return max_id

    def _handle_local(self, msg: Message, ctx: Context):
        owner_id = self._find_key_owner(msg['key'])

        message_body = {
            'key': msg['key'],
        }
        if msg.type == 'PUT':
            message_body['value'] = msg['value']

        ctx.send(Message(f'{msg.type}_REQ', message_body), owner_id)

    def _handle_node_add(self, msg: Message, ctx: Context):
        to_send = {}

        for key in list(self._data.keys()):
            if self._find_key_owner(key) == msg['id']:
                to_send[key] = self._data.pop(key)

        ctx.send(Message('GET_KEYS', {'data': to_send}), msg['id'])

    def _handle_node_remove(self, msg: Message, ctx: Context):
        if self._id != msg['id']:
            return

        users = {}  # id -> to send dict
        for key in list(self._data.keys()):
            owner_id = self._find_key_owner(key)

            users.setdefault(owner_id, {})

            users[owner_id][key] = self._data.pop(key)

        for owner_id in users:
            to_send = users[owner_id]
            ctx.send(Message('GET_KEYS', {'data': to_send}), owner_id)

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type in ['GET', 'PUT', 'DELETE']:
            self._handle_local(msg, ctx)

        # Notification that a new node is added to the system.
        # Request:
        #   NODE_ADDED {"id": "node id"}
        # Response:
        #   N/A
        elif msg.type == 'NODE_ADDED':
            self._nodes.add(msg['id'])
            self._handle_node_add(msg, ctx)

        # Notification that a node is removed from the system.
        # Request:
        #   NODE_REMOVED {"id": "node id"}
        # Response:
        #   N/A
        elif msg.type == 'NODE_REMOVED':
            self._nodes.remove(msg['id'])
            self._handle_node_remove(msg, ctx)

        # Get number of records stored on the node.
        # Request:
        #   COUNT_RECORDS {}
        # Response:
        #   COUNT_RECORDS_RESP {"count": 100}
        elif msg.type == 'COUNT_RECORDS':
            resp = Message('COUNT_RECORDS_RESP', {
                'count': len(self._data)
            })
            ctx.send_local(resp)

        # Get keys of records stored on the node.
        # Request:
        #   DUMP_KEYS {}
        # Response:
        #   DUMP_KEYS_RESP {"keys": ["key1", "key2", ...]}
        elif msg.type == 'DUMP_KEYS':
            resp = Message('DUMP_KEYS_RESP', {
                'keys': list(self._data.keys())
            })
            ctx.send_local(resp)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == 'GET_REQ':
            key = msg['key']
            value = self._data.get(key)
            resp = Message('GET_ACK', {
                'key': key,
                'value': value
            })
            ctx.send(resp, sender)

        elif msg.type == 'PUT_REQ':
            key = msg['key']
            value = msg['value']
            self._data[key] = value
            resp = Message('PUT_ACK', {
                'key': key,
                'value': value
            })
            ctx.send(resp, sender)

        elif msg.type == 'DELETE_REQ':
            key = msg['key']
            value = self._data.pop(key, None)
            resp = Message('DELETE_ACK', {
                'key': key,
                'value': value
            })
            ctx.send(resp, sender)

        elif 'ACK' in msg.type:
            msg = Message(
                msg.type.replace('ACK', 'RESP'),
                {
                    'key': msg['key'],
                    'value': msg['value'],
                }
            )
            ctx.send_local(msg)

        elif msg.type == 'GET_KEYS':
            for key, value in msg['data'].items():
                self._data[key] = value

    def on_timer(self, timer_name: str, ctx: Context):
        pass