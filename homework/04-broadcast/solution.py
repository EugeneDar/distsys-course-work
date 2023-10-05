from dslabmp import Context, Message, Process
from typing import List


# TODO maybe if we are not a sender, and we got bcast we can deliver it without count checking
class BroadcastProcess(Process):
    def __init__(self, proc_id: str, processes: List[str]):
        self._id = proc_id
        self._processes = processes

        self._messages_broadcasted = set()  # hashes
        self._received_messages_hashes = set()  # got from local user
        self._delivered_messages_hashes = set()  # sent to local user
        self._messages_want_deliver = {}  # hash -> [approves_count, message]

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == 'SEND':
            message_hash = hash(msg['text'])

            bcast_msg = Message('BCAST', {
                'text': msg['text'],
                'hashes_need_before': list(self._received_messages_hashes | self._delivered_messages_hashes)
            })
            for proc in self._processes:
                if proc == self._id:
                    continue
                ctx.send(bcast_msg, proc)
            self._received_messages_hashes.add(message_hash)

            self._messages_broadcasted.add(message_hash)

            self._messages_want_deliver[message_hash] = [0, bcast_msg]

    def try_send_messages(self, ctx):
        messages_sent = 0

        for message_hash in list(self._messages_want_deliver.keys()):
            pair = self._messages_want_deliver[message_hash]
            approves_count, message = pair[0], pair[1]

            if approves_count < len(self._processes) // 2:
                continue

            if not set(message['hashes_need_before']).issubset(self._delivered_messages_hashes):
                continue
            deliver_msg = Message('DELIVER', {
                'text': message['text']
            })
            ctx.send_local(deliver_msg)
            messages_sent += 1

            del self._messages_want_deliver[message_hash]
            self._delivered_messages_hashes.add(message_hash)

        return messages_sent > 0

    def on_message(self, msg: Message, sender: str, ctx: Context):
        message_hash = hash(msg['text'])

        if msg.type == 'BCAST':
            if message_hash in self._delivered_messages_hashes:
                return

            if message_hash in self._messages_want_deliver:
                self._messages_want_deliver[message_hash] = [
                    self._messages_want_deliver[message_hash][0] + 1,
                    self._messages_want_deliver[message_hash][1]
                ]
            else:
                self._messages_want_deliver[message_hash] = [1, msg]

            if message_hash not in self._messages_broadcasted:
                for proc in self._processes:
                    if proc == self._id:
                        continue
                    ctx.send(msg, proc)
                self._messages_broadcasted.add(message_hash)

            while True:
                sent = self.try_send_messages(ctx)
                if not sent:
                    break

    def on_timer(self, timer_name: str, ctx: Context):
        pass
