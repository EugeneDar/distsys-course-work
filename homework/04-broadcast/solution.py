from dslabmp import Context, Message, Process
from typing import List


class BroadcastProcess(Process):
    def __init__(self, proc_id: str, processes: List[str]):
        self._id = proc_id
        self._processes = processes
        self._sent_messages_hashes = set()
        self._messages_want_send = {}  # hash -> approves count

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == 'SEND':
            bcast_msg = Message('BCAST', {
                'text': msg['text']
            })
            for proc in self._processes:
                if proc == self._id:
                    continue
                ctx.send(bcast_msg, proc)

            message_hash = hash(msg['text'])
            self._messages_want_send[message_hash] = 0

    def try_send_local_message(self, msg, ctx):
        message_hash = hash(msg['text'])
        if self._messages_want_send[message_hash] >= len(self._processes) // 2:
            deliver_msg = Message('DELIVER', {
                'text': msg['text']
            })
            ctx.send_local(deliver_msg)

            del self._messages_want_send[message_hash]
            self._sent_messages_hashes.add(message_hash)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        message_hash = hash(msg['text'])

        if msg.type == 'BCAST':
            if message_hash in self._sent_messages_hashes:
                return

            if message_hash in self._messages_want_send:
                self._messages_want_send[message_hash] += 1
                self.try_send_local_message(msg, ctx)
                return

            self._messages_want_send[message_hash] = 1
            self.try_send_local_message(msg, ctx)

            bcast_msg = Message('BCAST', {
                'text': msg['text']
            })
            for proc in self._processes:
                if proc == self._id:
                    continue
                ctx.send(bcast_msg, proc)

    def on_timer(self, timer_name: str, ctx: Context):
        pass
