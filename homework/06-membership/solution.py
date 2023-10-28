from dslabmp import Context, Message, Process
import random

JOIN = 'JOIN'

ACK = 'ACK'
PING = 'PING'
PING_REQ = 'PING_REQ'

FIRST_PHASE = 'first phase'
SECOND_PHASE = 'second phase'
THIRD_PHASE = 'third phase'

RESPONSE_TIME = 0.21
SLEEP_TIME = RESPONSE_TIME
K = 3


ALIVE = 1
LEFT = 4
DEAD = 5


class GroupMember(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id
        self._group = {}  # id -> status
        self._in_group_now = False

        self._phase_start_time = 0
        self._suspected_id = None
        self._got_suspected_ack = False

    def _process_local_join(self, msg: Message, ctx: Context):
        self._in_group_now = True

        seed = msg['seed']
        if seed == self._id:
            self._group.clear()
        else:
            ctx.send(Message(JOIN, {
                'newcomer': self._id
            }), seed)
            self._group = {
                seed: ALIVE
            }

        ctx.set_timer(FIRST_PHASE, SLEEP_TIME)

    def _process_local_leave(self, msg: Message, ctx: Context):
        for timer_name in [FIRST_PHASE, SECOND_PHASE, THIRD_PHASE]:
            ctx.cancel_timer(timer_name)

        self._in_group_now = False
        self._group.clear()

    def _process_local_get_members(self, msg: Message, ctx: Context) -> list:
        return [
            node_id
            for node_id, status in self._group.items()
            if status == ALIVE
        ] + [self._id]

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == 'JOIN':

            self._process_local_join(msg, ctx)

        elif msg.type == 'LEAVE':

            self._process_local_leave(msg, ctx)

        elif msg.type == 'GET_MEMBERS':

            members = self._process_local_get_members(msg, ctx)
            ctx.send_local(Message('MEMBERS', {'members': members}))

    def _create_multicast_info(self):
        nodes_ids = random.sample(list(self._group.keys()), min(K, len(self._group)))
        return {
            node_id: self._group[node_id]
            for node_id in nodes_ids
        }

    def _apply_multicast_info(self, info):
        for node_id, status in info.items():
            if node_id == self._id:
                continue

            if node_id not in self._group:
                # print(f'{self._id} mark {node_id} as {status}. 1')
                self._group[node_id] = status
                continue

            # node_id in self._group
            if status == DEAD or self._group[node_id] == DEAD:
                # print(f'{self._id} mark {node_id} as DEAD. 2')
                self._group[node_id] = DEAD
            else:
                # print(f'{self._id} mark {node_id} as ALIVE. 3')
                self._group[node_id] = ALIVE

    def _process_ping(self, msg: Message, sender: str, ctx: Context):
        """
        BODY: {
            'requester'
            'multicast info'
        }
        """

        self._apply_multicast_info(msg['multicast info'])

        ctx.send(Message(
            ACK,
            {
                'confirmation sender': self._id,
                'requester': msg['requester'],
            }
        ), sender)

    def _process_ping_req(self, msg: Message, sender: str, ctx: Context):
        """
        BODY: {
            'suspect'
            'multicast info'
        }
        """

        self._apply_multicast_info(msg['multicast info'])

        ctx.send(Message(
            PING,
            {
                'requester': sender,
                'multicast info': msg['multicast info'],
            }
        ), msg['suspect'])

    def _process_ack(self, msg: Message, sender: str, ctx: Context):
        """
        BODY: {
            'requester'
            'confirmation sender'
        }
        """

        if msg['requester'] == self._id:
            if self._suspected_id == msg['confirmation sender']:
                self._got_suspected_ack = True
        else:
            ctx.send(Message(
                ACK,
                {
                    'confirmation sender': msg['confirmation sender'],
                    'requester': msg['requester'],
                }
            ), msg['requester'])

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # if we left group
        if not self._in_group_now:
            return

        if msg.type == JOIN:
            nodes_ids = random.sample(list(self._group.keys()), min(K, len(self._group)))

            for node_id in nodes_ids:
                ctx.send(Message(
                    PING_REQ,
                    {
                        'suspect': msg['newcomer'],
                        'multicast info': {
                            msg['newcomer']: ALIVE
                        },
                    }
                ), node_id)

            self._group[sender] = ALIVE

        if msg.type == PING:

            self._process_ping(msg, sender, ctx)

        elif msg.type == PING_REQ:

            self._process_ping_req(msg, sender, ctx)

        elif msg.type == ACK:

            self._process_ack(msg, sender, ctx)

    def _send_one_random_ping(self, ctx):
        self._phase_start_time = ctx.time()
        self._got_suspected_ack = False
        self._suspected_id = random.choice(list(self._group.keys()))

        ctx.send(Message(
            PING,
            {
                'requester': self._id,
                'multicast info': self._create_multicast_info(),
            }
        ),  self._suspected_id)

    def _send_random_ping_requests(self, ctx):
        self._phase_start_time = ctx.time()

        nodes_ids = random.sample(list(self._group.keys()), min(K + 1, len(self._group)))
        if self._suspected_id not in nodes_ids:
            nodes_ids = nodes_ids[:-1]
        else:
            nodes_ids = set(nodes_ids)
            nodes_ids.remove(self._suspected_id)

        for node_id in nodes_ids:
            ctx.send(Message(
                PING_REQ,
                {
                    'suspect': self._suspected_id,
                    'multicast info': self._create_multicast_info(),
                }
            ), node_id)

    def on_timer(self, timer_name: str, ctx: Context):
        if timer_name == FIRST_PHASE:
            """
            Send one ping
            """

            # sleep again if there is no members
            if not self._group:
                ctx.set_timer(FIRST_PHASE, SLEEP_TIME)
                return

            self._send_one_random_ping(ctx)
            ctx.set_timer(SECOND_PHASE, SLEEP_TIME)

        elif timer_name == SECOND_PHASE:
            """
            Send ping_reqs if there is no ack
            """

            # sleep again if woke up too early
            if ctx.time() - self._phase_start_time < RESPONSE_TIME:
                ctx.set_timer(SECOND_PHASE, SLEEP_TIME)
                return

            if not self._got_suspected_ack:
                self._send_random_ping_requests(ctx)
            ctx.set_timer(THIRD_PHASE, SLEEP_TIME)

            return

        elif timer_name == THIRD_PHASE:
            """
            Mark as failed if there is no ack
            """

            # sleep again if woke up too early
            if ctx.time() - self._phase_start_time < RESPONSE_TIME:
                ctx.set_timer(SECOND_PHASE, SLEEP_TIME)
                return

            if not self._got_suspected_ack:
                # print(f'{self._id} mark {self._suspected_id} as DEAD. 0')
                self._group[self._suspected_id] = DEAD
            else:
                self._group[self._suspected_id] = ALIVE
            self._suspected_id = None

            ctx.set_timer(FIRST_PHASE, SLEEP_TIME)
