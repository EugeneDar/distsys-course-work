from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SendRequest(_message.Message):
    __slots__ = ["author", "text"]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    author: str
    text: str
    def __init__(self, author: _Optional[str] = ..., text: _Optional[str] = ...) -> None: ...

class SendResponse(_message.Message):
    __slots__ = ["sendTime"]
    SENDTIME_FIELD_NUMBER: _ClassVar[int]
    sendTime: _timestamp_pb2.Timestamp
    def __init__(self, sendTime: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ReadRequest(_message.Message):
    __slots__ = []
    def __init__(self) -> None: ...

class MessageFromStreamRead(_message.Message):
    __slots__ = ["author", "text", "sendTime"]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    TEXT_FIELD_NUMBER: _ClassVar[int]
    SENDTIME_FIELD_NUMBER: _ClassVar[int]
    author: str
    text: str
    sendTime: _timestamp_pb2.Timestamp
    def __init__(self, author: _Optional[str] = ..., text: _Optional[str] = ..., sendTime: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...
