from dslabmp import Context, Message, Process
from typing import List


# TODO maybe if we are not a sender, and we got bcast we can deliver it without count checking
class BroadcastProcess(Process):
    def __init__(self, proc_id: str, processes: List[str]):
        self._id = proc_id
        self._processes = processes
        self._counter = 0

        self._messages_broadcasted = set()  # ids
        self._received_messages_ids = set()  # got from local user
        self._delivered_messages_ids = set()  # sent to local user
        self._messages_want_deliver = {}  # ids -> [approves_count, message]

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == 'SEND':
            message_id = str(self._counter) + '_' + str(self._id)
            self._counter += 1

            bcast_msg = Message('BCAST', {
                'text': msg['text'],
                'ids_need_before': list(self._received_messages_ids | self._delivered_messages_ids),
                'id': message_id
            })
            for proc in self._processes:
                if proc == self._id:
                    continue
                ctx.send(bcast_msg, proc)
            self._received_messages_ids.add(message_id)

            self._messages_broadcasted.add(message_id)

            self._messages_want_deliver[message_id] = [0, bcast_msg]

    def try_deliver_messages(self, ctx):
        messages_sent = 0

        for message_id in list(self._messages_want_deliver.keys()):
            pair = self._messages_want_deliver[message_id]
            approves_count, message = pair[0], pair[1]

            if approves_count < len(self._processes) // 2:
                continue

            if not set(message['ids_need_before']).issubset(self._delivered_messages_ids):
                continue
            deliver_msg = Message('DELIVER', {
                'text': message['text']
            })
            ctx.send_local(deliver_msg)
            messages_sent += 1

            del self._messages_want_deliver[message_id]
            self._delivered_messages_ids.add(message_id)

        return messages_sent > 0

    def on_message(self, msg: Message, sender: str, ctx: Context):
        message_id = msg['id']

        if msg.type == 'BCAST':
            if message_id in self._delivered_messages_ids:
                return

            if message_id in self._messages_want_deliver:
                self._messages_want_deliver[message_id] = [
                    self._messages_want_deliver[message_id][0] + 1,
                    self._messages_want_deliver[message_id][1]
                ]
            else:
                self._messages_want_deliver[message_id] = [1, msg]

            if message_id not in self._messages_broadcasted:
                for proc in self._processes:
                    if proc == self._id:
                        continue
                    ctx.send(msg, proc)
                self._messages_broadcasted.add(message_id)

            while True:
                sent = self.try_deliver_messages(ctx)
                if not sent:
                    break

    def on_timer(self, timer_name: str, ctx: Context):
        pass
