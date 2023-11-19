import hashlib
from dslabmp import Context, Message, Process
from typing import List

TIMER_TIME = 0.5

KEY = 'key'
VALUE = 'value'
QUORUM = 'quorum'
QUORUM_ID = 'quorum_id'
ANSWERS = 'answers'
OPERATION_TIME = 'operation_time'
REQUEST_INFO = 'request_info'
SENDER = 'sender'
REQUEST_TYPE = 'request_type'
REAL_REPLICA = 'real_replica'
HH_ID = 'hh_id'


class StorageNode(Process):
    def __init__(self, node_id: str, nodes: List[str]):
        self._id = node_id
        self._nodes = nodes
        self._quorum_counter = 0
        self._hh_counter = 0

        """
        KEY -> VALUE
        """
        self._data = {}

        """
        KEY -> OPERATION TIME
        """
        self._operations_times = {}

        """
        QUORUM_ID -> {QUORUM: number, ANSWERS: [], REQUEST_INFO: {}}
        """
        self._quorum = {}

        """
        HH_ID -> {REQUEST_INFO: {}, REAL_REPLICA: id}
        """
        self._hinted_handoff_queue = {}

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

    def _refresh_stale_nodes(self, msg: Message, sender: str, ctx: Context):
        quorum_result = self._take_quorum_result(msg)

        quorum_id = msg[QUORUM_ID]
        operation_time = max(answer[OPERATION_TIME] for answer in self._quorum[quorum_id][ANSWERS])

        if quorum_result[VALUE] != msg[VALUE]:
            ctx.send(
                Message('REFRESH', {
                    KEY: quorum_result[KEY],
                    VALUE: quorum_result[VALUE],
                    OPERATION_TIME: operation_time,
                }),
                sender
            )

    def _launch_hinted_handoff(self, msg: Message, sender: str, ctx: Context):
        if self._id in get_key_replicas(msg[KEY], len(self._nodes)):
            return

        hh_id = str(self._hh_counter)
        self._hh_counter += 1

        self._hinted_handoff_queue[hh_id] = {
            REQUEST_INFO: {
                REQUEST_TYPE: msg.type.replace('REQ', 'REFRESH'),
                KEY: msg[KEY],
                VALUE: msg[VALUE],
                OPERATION_TIME: ctx.time(),
            },
            REAL_REPLICA: msg[REAL_REPLICA],
        }

        ctx.set_timer_once('hh', TIMER_TIME)

    def _refresh_node(self, msg):
        key = msg[KEY]

        won_operation_time, won_value = self._solve_conflicts(
            self._operations_times.get(key, -1),
            self._data.get(key),
            msg[OPERATION_TIME],
            msg[VALUE]
        )

        self._data[key] = won_value
        self._operations_times[key] = won_operation_time

        return won_value, won_operation_time

    def _create_quorum(self, quorum_size, msg, ctx) -> str:
        self._quorum_counter += 1

        quorum_id = str(self._quorum_counter)

        self._quorum[quorum_id] = {
            QUORUM: quorum_size,
            ANSWERS: [],
            REQUEST_INFO: {
                REQUEST_TYPE: msg.type,
                KEY: msg[KEY],
                VALUE: msg[VALUE] if msg.type == 'PUT' else None,
                OPERATION_TIME: ctx.time(),
            }
        }

        return quorum_id

    def _add_answer_to_quorum(self, msg, sender):
        key = msg[KEY]
        value = msg[VALUE]
        quorum_id = msg[QUORUM_ID]
        operation_time = msg[OPERATION_TIME]

        self._quorum[quorum_id][ANSWERS].append({
            KEY: key,
            VALUE: value,
            OPERATION_TIME: operation_time,
            SENDER: sender,
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

        return {
            KEY: newest_answer[KEY],
            VALUE: newest_answer[VALUE],
        }

    def _handle_local_get(self, msg: Message, ctx: Context):
        key = msg[KEY]
        quorum = msg[QUORUM]
        replicas = get_key_replicas(key, len(self._nodes))

        quorum_id = self._create_quorum(quorum, msg, ctx)

        for replica in replicas:
            ctx.send(
                Message('GET_REQ', {
                    KEY: key,
                    QUORUM_ID: quorum_id,
                }),
                replica
            )

        ctx.set_timer('sq' + quorum_id, TIMER_TIME)

    def _handle_local_put(self, msg: Message, ctx: Context):
        key = msg[KEY]
        value = msg[VALUE]
        quorum = msg[QUORUM]
        replicas = get_key_replicas(key, len(self._nodes))

        quorum_id = self._create_quorum(quorum, msg, ctx)

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

        ctx.set_timer('sq' + quorum_id, TIMER_TIME)

    def _handle_local_delete(self, msg: Message, ctx: Context):
        key = msg[KEY]
        quorum = msg[QUORUM]
        replicas = get_key_replicas(key, len(self._nodes))

        quorum_id = self._create_quorum(quorum, msg, ctx)

        for replica in replicas:
            ctx.send(
                Message('DELETE_REQ', {
                    KEY: key,
                    QUORUM_ID: quorum_id,
                    OPERATION_TIME: ctx.time(),
                }),
                replica
            )

        ctx.set_timer('sq' + quorum_id, TIMER_TIME)

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
        self._launch_hinted_handoff(msg, sender, ctx)

        key = msg[KEY]
        quorum_id = msg[QUORUM_ID]

        won_value, won_operation_time = self._refresh_node(msg)

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
        if msg[QUORUM_ID] not in self._quorum:
            print('Message not from my quorum')
            return

        self._add_answer_to_quorum(msg, sender)

        self._refresh_stale_nodes(msg, sender, ctx)

        if not self._have_enough_quorum(msg):
            return

        quorum_result = self._take_quorum_result(msg)

        ctx.send_local(
            Message(msg.type.replace('ANSWER', 'RESP'), quorum_result)
        )

    def _handle_put_refresh(self, msg: Message, sender: str, ctx: Context):
        self._refresh_node(msg)

        ctx.send(
            Message('REFRESH_ACK', {
                HH_ID: msg[HH_ID]
            }),
            sender
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

        elif msg.type == 'REFRESH':
            self._refresh_node(msg)

        elif msg.type == 'PUT_REFRESH':
            self._handle_put_refresh(msg, sender, ctx)

        elif msg.type == 'REFRESH_ACK':
            self._hinted_handoff_queue.pop(msg[HH_ID])

    def __find_new_replicas_info(self, quorum_id):
        who_answered = [answer[SENDER] for answer in self._quorum[quorum_id][ANSWERS]]

        key = self._quorum[quorum_id][REQUEST_INFO][KEY]

        real_replicas = get_key_replicas(key, len(self._nodes))

        new_replicas = []
        some_pointer = int(real_replicas[-1])
        while len(new_replicas) != 3 - len(who_answered):
            some_pointer = get_next_replica(some_pointer, len(self._nodes))
            new_replicas.append(str(some_pointer))

        ancestors = []
        real_replicas_pointer = 0
        while len(ancestors) != len(new_replicas):
            while real_replicas[real_replicas_pointer] in who_answered:
                real_replicas_pointer += 1
            ancestors.append(real_replicas[real_replicas_pointer])
            real_replicas_pointer += 1

        return new_replicas, ancestors

    def _sloppy_quorum_timer(self, timer_name: str, ctx: Context):
        quorum_id = timer_name[2:]
        quorum_have = len(self._quorum[quorum_id][ANSWERS])

        if quorum_have >= 3:
            return

        request_info = self._quorum[quorum_id][REQUEST_INFO]

        request_type = request_info[REQUEST_TYPE] + '_REQ'
        key = request_info[KEY]
        value = request_info[VALUE]
        operation_time = request_info[OPERATION_TIME]

        new_replicas, ancestors = self.__find_new_replicas_info(quorum_id)

        for new_replica, ancestor in zip(new_replicas, ancestors):
            ctx.send(
                Message(request_type, {
                    KEY: key,
                    VALUE: value,
                    QUORUM_ID: quorum_id,
                    OPERATION_TIME: operation_time,
                    REAL_REPLICA: ancestor,
                }),
                new_replica
            )

    def _hinted_handoff_timer(self, timer_name: str, ctx: Context):
        for hh_id in list(self._hinted_handoff_queue.keys()):
            info = self._hinted_handoff_queue[hh_id]

            request_info = info[REQUEST_INFO]
            real_replica = info[REAL_REPLICA]

            # todo think about it
            if request_info[REQUEST_TYPE] != 'PUT_REFRESH':
                self._hinted_handoff_queue.pop(hh_id)
                continue

            ctx.send(
                Message(request_info[REQUEST_TYPE], {
                    KEY: request_info[KEY],
                    VALUE: request_info[VALUE],
                    OPERATION_TIME: request_info[OPERATION_TIME],
                    HH_ID: hh_id,
                }),
                real_replica
            )

        if len(self._hinted_handoff_queue) > 0:
            ctx.set_timer_once('hh', TIMER_TIME)

    def on_timer(self, timer_name: str, ctx: Context):
        """
        Данный таймер завязан на тот факт,
        что ключ всегда должен оказаться в трех репликах.

        И если этого не случилось, то запрос нужно отправить
        и остальным узлам.

        Вероятно достаточно просто отправить им аналогичный запрос
        и не делать больше ничего лишнего.
        """
        if timer_name.startswith('sq'):
            self._sloppy_quorum_timer(timer_name, ctx)

        elif timer_name.startswith('hh'):
            self._hinted_handoff_timer(timer_name, ctx)


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
