from dslabmp import Context, Message, Process


# AT MOST ONCE ---------------------------------------------------------------------------------------------------------

class AtMostOnceSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id
        self._need_confirm = dict()

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user
        msg['time'] = 0
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

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()

        # ctx.send_local(msg)
        pass

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
        ctx.send(msg, self._receiver)

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

import sys

def get_size(obj, seen=None):
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__slots__'):
        size += sum([get_size(getattr(obj, slot), seen) for slot in obj.__slots__])
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size

# EXACTLY ONCE ---------------------------------------------------------------------------------------------------------

class ExactlyOnceSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


class ExactlyOnceReceiver(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()
        pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


# EXACTLY ONCE + ORDERED -----------------------------------------------------------------------------------------------

class ExactlyOnceOrderedSender(Process):
    def __init__(self, proc_id: str, receiver_id: str):
        self._id = proc_id
        self._receiver = receiver_id

    def on_local_message(self, msg: Message, ctx: Context):
        # receive message for delivery from local user
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


class ExactlyOnceOrderedReceiver(Process):
    def __init__(self, proc_id: str):
        self._id = proc_id

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver
        # deliver message to local user with ctx.send_local()
        pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass
