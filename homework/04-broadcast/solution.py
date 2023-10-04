from dslabmp import Context, Message, Process
from typing import List


class BroadcastProcess(Process):
    def __init__(self, proc_id: str, processes: List[str]):
        self._id = proc_id
        self._processes = processes
        self._messages_hashes_history = set()

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == 'SEND':
            bcast_msg = Message('BCAST', {
                'text': msg['text']
            })
            # best-effort broadcast
            for proc in self._processes:
                if proc == self._id:
                    continue
                ctx.send(bcast_msg, proc)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        message_hash = hash(msg['text'])
        if message_hash in self._messages_hashes_history:
            return
        self._messages_hashes_history.add(message_hash)

        if msg.type == 'BCAST':
            # deliver message to the local user
            deliver_msg = Message('DELIVER', {
                'text': msg['text']
            })
            ctx.send_local(deliver_msg)

        bcast_msg = Message('BCAST', {
            'text': msg['text']
        })
        for proc in self._processes:
            ctx.send(bcast_msg, proc)



    def on_timer(self, timer_name: str, ctx: Context):
        pass
