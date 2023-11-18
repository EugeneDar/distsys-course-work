import hashlib
from dslabmp import Context, Message, Process
from typing import List

KEY = 'key'
VALUE = 'value'
QUORUM = 'quorum'
QUORUM_ID = 'quorum_id'
ANSWERS = 'answers'
OPERATION_TIME = 'operation_time'


class StorageNode(Process):
    def __init__(self, node_id: str, nodes: List[str]):
        self._id = node_id
        self._nodes = nodes
        self._quorum_counter = 0

        """
        KEY -> VALUE
        """
        self._data = {}

        """
        KEY -> OPERATION TIME
        """
        self._operations_times = {}

        """
        QUORUM_ID -> {QUORUM: number, ANSWERS: []}
        """
        self._quorum = {}

    def _solve_conflicts(self, first_op_time, first_value, second_op_time, second_value) -> tuple[int, str]:
        if first_op_time > second_op_time:
            return first_op_time, first_value
        elif second_op_time > first_op_time:
            return second_op_time, second_value

        if not first_value and not second_value:
            return first_op_time, first_value

        if (first_value or "") > (second_value or ""):
            return first_op_time, first_value
        else:
            return second_op_time, second_value

    def _create_quorum(self, quorum_size, replicas) -> str:
        self._quorum_counter += 1

        quorum_id = str(self._quorum_counter)

        self._quorum[quorum_id] = {
            QUORUM: quorum_size,
            ANSWERS: []
        }

        return quorum_id

    def _add_answer_to_quorum(self, msg):
        key = msg[KEY]
        value = msg[VALUE]
        quorum_id = msg[QUORUM_ID]
        operation_time = msg[OPERATION_TIME]

        self._quorum[quorum_id][ANSWERS].append({
            KEY: key,
            VALUE: value,
            OPERATION_TIME: operation_time,
        })

    def _have_enough_quorum(self, msg) -> bool:
        quorum_id = msg[QUORUM_ID]
        # if you use >= then will answer many times
        return len(self._quorum[quorum_id][ANSWERS]) == self._quorum[quorum_id][QUORUM]

    def _take_quorum_result(self, msg):
        quorum_id = msg[QUORUM_ID]

        newest_answer = None
        for answer in self._quorum[quorum_id][ANSWERS]:
            if (
                not newest_answer
                or answer[OPERATION_TIME] > newest_answer[OPERATION_TIME]
                or (
                    answer[OPERATION_TIME] == newest_answer[OPERATION_TIME]
                    and (answer[VALUE] or "") > (newest_answer[VALUE] or "")
                )
            ):
                newest_answer = answer

        newest_answer.pop(OPERATION_TIME)
        return newest_answer

    def _handle_local_get(self, msg: Message, ctx: Context):
        key = msg[KEY]
        quorum = msg[QUORUM]
        replicas = get_key_replicas(key, len(self._nodes))

        quorum_id = self._create_quorum(quorum, replicas)

        for replica in replicas:
            ctx.send(
                Message('GET_REQ', {
                    KEY: key,
                    QUORUM_ID: quorum_id,
                }),
                replica
            )

    def _handle_local_put(self, msg: Message, ctx: Context):
        key = msg[KEY]
        value = msg[VALUE]
        quorum = msg[QUORUM]
        replicas = get_key_replicas(key, len(self._nodes))

        quorum_id = self._create_quorum(quorum, replicas)

        for replica in replicas:
            ctx.send(
                Message('PUT_REQ', {
                    KEY: key,
                    VALUE: value,
                    QUORUM_ID: quorum_id,
                    OPERATION_TIME: ctx.time(),
                }),
                replica
            )

    def _handle_local_delete(self, msg: Message, ctx: Context):
        key = msg[KEY]
        quorum = msg[QUORUM]
        replicas = get_key_replicas(key, len(self._nodes))

        quorum_id = self._create_quorum(quorum, replicas)

        for replica in replicas:
            ctx.send(
                Message('DELETE_REQ', {
                    KEY: key,
                    QUORUM_ID: quorum_id,
                    OPERATION_TIME: ctx.time(),
                }),
                replica
            )

    def on_local_message(self, msg: Message, ctx: Context):
        # Get key value.
        # Request:
        #   GET {"key": "some key", "quorum": 1-3}
        # Response:
        #   GET_RESP {"key": "some key", "value": "value for this key"}
        #   GET_RESP {"key": "some key", "value": null} - if record for this key is not found
        if msg.type == 'GET':
            self._handle_local_get(msg, ctx)

        # Store (key, value) record
        # Request:
        #   PUT {"key": "some key", "value: "some value", "quorum": 1-3}
        # Response:
        #   PUT_RESP {"key": "some key", "value: "some value"}
        elif msg.type == 'PUT':
            self._handle_local_put(msg, ctx)

        # Delete value for the key
        # Request:
        #   DELETE {"key": "some key", "quorum": 1-3}
        # Response:
        #   DELETE_RESP {"key": "some key", "value": "some value"}
        elif msg.type == 'DELETE':
            self._handle_local_delete(msg, ctx)

    def _handle_get_req(self, msg: Message, sender: str, ctx: Context):
        key = msg[KEY]
        quorum_id = msg[QUORUM_ID]
        operation_time = self._operations_times.get(key, -1)

        value = self._data.get(key)

        ctx.send(
            Message('GET_ANSWER', {
                KEY: key,
                VALUE: value,
                QUORUM_ID: quorum_id,
                OPERATION_TIME: operation_time,
            }),
            sender
        )

    def _handle_put_req(self, msg: Message, sender: str, ctx: Context):
        key = msg[KEY]
        quorum_id = msg[QUORUM_ID]

        won_operation_time, won_value = self._solve_conflicts(
            self._operations_times.get(key, -1),
            self._data.get(key),
            msg[OPERATION_TIME],
            msg[VALUE]
        )

        self._data[key] = won_value
        self._operations_times[key] = won_operation_time

        ctx.send(
            Message('PUT_ANSWER', {
                KEY: key,
                VALUE: won_value,
                QUORUM_ID: quorum_id,
                OPERATION_TIME: won_operation_time,
            }),
            sender
        )

    def _handle_delete_req(self, msg: Message, sender: str, ctx: Context):
        key = msg[KEY]
        quorum_id = msg[QUORUM_ID]

        value = self._data.pop(key, None)
        # TODO if operation time of put less then current key operation time, then do nothing
        self._operations_times[key] = msg[OPERATION_TIME]

        ctx.send(
            Message('DELETE_ANSWER', {
                KEY: key,
                VALUE: value,
                QUORUM_ID: quorum_id,
                OPERATION_TIME: msg[OPERATION_TIME],
            }),
            sender
        )

    def _handle_answer(self, msg: Message, sender: str, ctx: Context):
        self._add_answer_to_quorum(msg)
        if not self._have_enough_quorum(msg):
            return

        quorum_result = self._take_quorum_result(msg)

        ctx.send_local(
            Message(msg.type.replace('ANSWER', 'RESP'), quorum_result)
        )

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == 'GET_REQ':
            self._handle_get_req(msg, sender, ctx)

        elif msg.type == 'PUT_REQ':
            self._handle_put_req(msg, sender, ctx)

        elif msg.type == 'DELETE_REQ':
            self._handle_delete_req(msg, sender, ctx)

        elif msg.type in ['GET_ANSWER', 'PUT_ANSWER', 'DELETE_ANSWER']:
            self._handle_answer(msg, sender, ctx)

    def on_timer(self, timer_name: str, ctx: Context):
        pass


def get_key_replicas(key: str, node_count: int):
    replicas = []
    key_hash = int.from_bytes(hashlib.md5(key.encode('utf8')).digest(), 'little', signed=False)
    cur = key_hash % node_count
    for _ in range(3):
        replicas.append(str(cur))
        cur = get_next_replica(cur, node_count)
    return replicas


def get_next_replica(i, node_count: int):
    return (i + 1) % node_count
