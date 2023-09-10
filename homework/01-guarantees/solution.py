from dslabmp import Context, Message, Process
from queue import Queue


# AT MOST ONCE ---------------------------------------------------------------------------------------------------------

class AtMostOnceSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._index = 0

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user
        msg['id'] = self._index
        self._index += 1
        ctx.send(msg, self._receiver)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


class AtMostOnceReceiver(Process):

    def __init__(self, proc_id: str):
        self._id = proc_id
        self._queue = []

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()
        received_index = msg['id']
        msg.remove('id')

        for index in self._queue:
            if index == received_index:
                return

        if len(self._queue) >= 15:
            self._queue.pop(0)
        self._queue.append(received_index)

        ctx.send_local(msg)

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


# AT LEAST ONCE --------------------------------------------------------------------------------------------------------

class AtLeastOnceSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._need_send = dict()  # {id: msg}
        self._index = 1

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user\

        self._need_send[self._index] = msg
        msg['id'] = self._index

        self._index += 1

        if len(self._need_send) == 1:
            ctx.set_timer('resend', 5)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        if msg['id'] in self._need_send:
            self._need_send.pop(msg['id'])
        if len(self._need_send) == 0:
            ctx.cancel_timer('resend')

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        if timer_name in {'resend'}:
            for index in self._need_send.keys():
                msg = self._need_send[index]
                msg['id'] = index
                ctx.send(msg, self._receiver)
            if len(self._need_send) != 0:
                ctx.set_timer('resend', 5)


class AtLeastOnceReceiver(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()
        index = msg['id']
        msg.remove('id')
        ctx.send_local(msg)

        ctx.send(Message('', {'id': index}), sender)

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


# EXACTLY ONCE ---------------------------------------------------------------------------------------------------------

class ExactlyOnceSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._need_send = list()  # {id: msg}
        self._index = 0

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user\

        msg['id'] = self._index
        self._need_send.append(msg)

        self._index += 1

        if len(self._need_send) == 1:
            ctx.set_timer('resend', 5)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        for i, value in enumerate(self._need_send):
            if value['id'] == msg['id']:
                self._need_send.pop(i)
                break
        if len(self._need_send) == 0:
            ctx.cancel_timer('resend')

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        send_buffer_size = 2

        if timer_name in {'resend'}:
            for i, msg in enumerate(self._need_send):
                if i >= send_buffer_size:
                    break
                ctx.send(msg, self._receiver)
            if len(self._need_send) > 0:
                ctx.set_timer('resend', 5)

class ExactlyOnceReceiver(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id
        self._queue = []

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()
        received_index = msg['id']
        msg.remove('id')

        ctx.send(Message('', {'id': received_index}), sender)

        for i, index in enumerate(self._queue):
            if index == received_index:
                self._queue.pop(i)
                self._queue.append(index)
                return

        if len(self._queue) >= 16:
            self._queue.pop(0)
        self._queue.append(received_index)

        ctx.send_local(msg)

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


# EXACTLY ONCE + ORDERED -----------------------------------------------------------------------------------------------

class ExactlyOnceOrderedSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._need_send = list()  # {id: msg}
        self._index = 0

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user\

        msg['id'] = self._index
        self._need_send.append(msg)

        self._index += 1

        if len(self._need_send) == 1:
            ctx.set_timer('resend', 8)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        for i, value in enumerate(self._need_send):
            if value['id'] == msg['id']:
                self._need_send.pop(i)
                break
        if len(self._need_send) == 0:
            ctx.cancel_timer('resend')

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        send_buffer_size = 2

        if timer_name in {'resend'}:
            for i, msg in enumerate(self._need_send):
                if i >= send_buffer_size:
                    break
                ctx.send(msg, self._receiver)
            if len(self._need_send) > 0:
                ctx.set_timer('resend', 8)


class ExactlyOnceOrderedReceiver(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id
        self._need_send_index = 0

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()
        received_index = msg['id']
        msg.remove('id')

        if received_index <= self._need_send_index:
            ctx.send(Message('', {'id': received_index}), sender)
        else:
            return

        if received_index == self._need_send_index:
            ctx.send_local(msg)
            self._need_send_index += 1

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass
