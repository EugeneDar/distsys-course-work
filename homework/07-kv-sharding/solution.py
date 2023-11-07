from dslabmp import Context, Message, Process
from typing import List
from collections import namedtuple
import random

Point = namedtuple("Point", "separator key_range successor_of_separator")

POW = 13
RING_SIZE = 2 ** POW
FINGERS_COUNT = POW


class Finger:
    def __init__(self, separator, key_range, successor_of_separator):
        self.separator = separator
        self.key_range = key_range
        self.successor_of_separator = successor_of_separator

    @staticmethod
    def in_range(key, key_range) -> bool:
        if key_range[0] <= key_range[1]:
            return key_range[0] <= key <= key_range[1]
        return key_range[0] <= key < RING_SIZE or 0 <= key <= key_range[1]

    def len(self):
        if self.key_range[0] <= self.key_range[1]:
            return self.key_range[1] - self.key_range[0] + 1
        return RING_SIZE - self.key_range[0] + self.key_range[1] + 1


class Cluster:
    def _build_all_init_cluster(self, nodes) -> list[Point]:
        random.seed(len(nodes))

        sample = random.sample(range(RING_SIZE), len(nodes))

        print(sample)

        cluster = []

        for index in range(len(nodes)):
            cluster.append((nodes[index], sample[index]))

        cluster = sorted(cluster, key=lambda value: value[1])

        for index, values in enumerate(cluster):
            next_index = (index + 1) % len(nodes)

            left_border = cluster[index][1]
            right_border = (cluster[next_index][1] - 1) % RING_SIZE

            yield Point(
                values[1],
                [left_border, right_border],
                # [
                #     (cluster[index - 1][1] + 1) % RING_SIZE,
                #     cluster[index][1]
                # ],
                values[0],
            )

    def __init__(self, nodes, my_id):
        self._fingers = []
        self._id = my_id

        all_init_cluster = list(self._build_all_init_cluster(list(nodes)))

        # if my_id == '0':
        #     print('#' * 30)
        #     for point in all_init_cluster:
        #         print(point.separator, point.successor_of_separator, point.key_range)
        #     print('#' * 30)

        prev_ranges = []
        for index, point in enumerate(all_init_cluster):
            left_border = all_init_cluster[index - 1].key_range[0] + 1
            right_border = point.key_range[0]
            if left_border == right_border and len(nodes) == 1:
                left_border, right_border = 0, RING_SIZE - 1
            prev_ranges.append([left_border, right_border])

        node_pointer = 0
        while all_init_cluster[node_pointer].successor_of_separator != my_id:
            node_pointer = (node_pointer + 1) % len(nodes)

        self.own_range = all_init_cluster[node_pointer].key_range

        current_pow = 1
        current_separator = all_init_cluster[node_pointer].separator + 1

        for finger_number in range(FINGERS_COUNT):
            while not Finger.in_range(current_separator, prev_ranges[node_pointer]):
                node_pointer = (node_pointer + 1) % len(nodes)

            self._fingers.append(Finger(
                current_separator,
                [current_separator, (current_separator + current_pow - 1) % RING_SIZE],
                all_init_cluster[node_pointer].successor_of_separator,
            ))

            current_separator = (current_separator + current_pow) % RING_SIZE
            current_pow *= 2

    def find_key_owner(self, key):
        print(self._id)
        print(f'key: {key}')
        # print(
            # [finger.key_range for finger in self._fingers]
        # )
        for finger in self._fingers:
            # print(finger.separator, finger.key_range, finger.successor_of_separator)
            if Finger.in_range(key, finger.key_range):
                # print('in')
                return finger
        raise ValueError("Successor of separator not found.")


class StorageNode(Process):
    def __init__(self, node_id: str, nodes: List[str]):
        self._id = node_id
        self._nodes = set(nodes)
        self._data = {}

        self._cluster = Cluster(list(self._nodes), self._id)

    def _handle_local_get(self, msg, ctx):
        self._handle_find_key_owner(
            Message(
                "FIND KEY OWNER",
                {
                    'who must answer': self._id,
                    'key': msg['key'],
                    'operation': 'GET',
                }
            ), None, ctx
        )

    def _handle_local_put(self, msg, ctx):
        self._handle_find_key_owner(
            Message(
                "FIND KEY OWNER",
                {
                    'who must answer': self._id,
                    'key': msg['key'],
                    'value': msg['value'],
                    'operation': 'PUT',
                }
            ), None, ctx
        )

    def _handle_local_delete(self, msg, ctx):
        self._handle_find_key_owner(
            Message(
                "FIND KEY OWNER",
                {
                    'who must answer': self._id,
                    'key': msg['key'],
                    'operation': 'DELETE',
                }
            ), None, ctx
        )

    def _handle_local_node_added(self, msg, ctx):
        self._nodes.add(msg['id'])
        # TODO
        pass

    def _handle_local_node_removed(self, msg, ctx):
        self._nodes.remove(msg['id'])
        # TODO
        pass

    def _handle_local_count_records(self, msg, ctx):
        resp = Message('COUNT_RECORDS_RESP', {
            'count': len(self._data)
        })
        ctx.send_local(resp)

    def _handle_local_dump_keys(self, msg, ctx):
        resp = Message('DUMP_KEYS_RESP', {
            'keys': list(self._data.keys())
        })
        ctx.send_local(resp)

    def on_local_message(self, msg: Message, ctx: Context):
        # Get value for the key.
        # Request:
        #   GET {"key": "some key"}
        # Response:
        #   GET_RESP {"key": "some key", "value": "value for this key"}
        #   GET_RESP {"key": "some key", "value": null} - if record for this key is not found
        if msg.type == 'GET':
            self._handle_local_get(msg, ctx)

        # Store (key, value) record.
        # Request:
        #   PUT {"key": "some key", "value: "some value"}
        # Response:
        #   PUT_RESP {"key": "some key", "value: "some value"}
        elif msg.type == 'PUT':
            self._handle_local_put(msg, ctx)

        # Delete value for the key.
        # Request:
        #   DELETE {"key": "some key"}
        # Response:
        #   DELETE_RESP {"key": "some key", "value": "some value"}
        elif msg.type == 'DELETE':
            self._handle_local_delete(msg, ctx)

        # Notification that a new node is added to the system.
        # Request:
        #   NODE_ADDED {"id": "node id"}
        # Response:
        #   N/A
        elif msg.type == 'NODE_ADDED':
            self._handle_local_node_added(msg, ctx)

        # Notification that a node is removed from the system.
        # Request:
        #   NODE_REMOVED {"id": "node id"}
        # Response:
        #   N/A
        elif msg.type == 'NODE_REMOVED':
            self._handle_local_node_removed(msg, ctx)

        # Get number of records stored on the node.
        # Request:
        #   COUNT_RECORDS {}
        # Response:
        #   COUNT_RECORDS_RESP {"count": 100}
        elif msg.type == 'COUNT_RECORDS':
            self._handle_local_count_records(msg, ctx)

        # Get keys of records stored on the node.
        # Request:
        #   DUMP_KEYS {}
        # Response:
        #   DUMP_KEYS_RESP {"keys": ["key1", "key2", ...]}
        elif msg.type == 'DUMP_KEYS':
            self._handle_local_dump_keys(msg, ctx)

    def _handle_you_are_key_owner(self, msg, sender, ctx):
        operation = msg['operation']

        if operation == 'GET':
            key = msg['key']
            value = self._data.get(key, None)
            resp = Message('GET_RESP', {
                'key': key,
                'value': value
            })
            ctx.send_local(resp)

        elif operation == 'PUT':
            key = msg['key']
            value = msg['value']
            self._data[key] = value
            resp = Message('PUT_RESP', {
                'key': key,
                'value': value
            })
            ctx.send_local(resp)

        elif operation == 'DELETE':
            key = msg['key']
            value = self._data.pop(key, None)
            resp = Message('DELETE_RESP', {
                'key': key,
                'value': value
            })
            ctx.send_local(resp)

    def _handle_find_key_owner(self, msg, sender, ctx):
        key = msg['key']
        operation = msg['operation']

        key_hash = hash(key) % RING_SIZE

        print(f'me: {self._id}, call {self._cluster.own_range}')
        finger_to_jump = self._cluster.find_key_owner(key_hash)

        message_type = "FIND KEY OWNER"
        if Finger.in_range(key_hash, self._cluster.own_range) or finger_to_jump.len() == 1:
            message_type = "YOU ARE KEY OWNER"

        message_body = {
            'who must answer': msg['who must answer'],
            'key': key,
            'operation': operation,
        }
        if operation == 'PUT':
            message_body['value'] = msg['value']

        ctx.send(
            Message(message_type, message_body),
            finger_to_jump.successor_of_separator
        )

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == "YOU ARE KEY OWNER":
            self._handle_you_are_key_owner(msg, sender, ctx)

        elif msg.type == "FIND KEY OWNER":
            self._handle_find_key_owner(msg, sender, ctx)

    def on_timer(self, timer_name: str, ctx: Context):
        pass
