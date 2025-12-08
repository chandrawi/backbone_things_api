from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class BufferSchema(_message.Message):
    __slots__ = ("id", "device_id", "model_id", "timestamp", "data_bytes", "data_type", "tag")
    ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DATA_BYTES_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    id: int
    device_id: bytes
    model_id: bytes
    timestamp: int
    data_bytes: bytes
    data_type: _containers.RepeatedScalarFieldContainer[int]
    tag: int
    def __init__(self, id: _Optional[int] = ..., device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., timestamp: _Optional[int] = ..., data_bytes: _Optional[bytes] = ..., data_type: _Optional[_Iterable[int]] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferMultipleSchema(_message.Message):
    __slots__ = ("schemas",)
    SCHEMAS_FIELD_NUMBER: _ClassVar[int]
    schemas: _containers.RepeatedCompositeFieldContainer[BufferSchema]
    def __init__(self, schemas: _Optional[_Iterable[_Union[BufferSchema, _Mapping]]] = ...) -> None: ...

class BufferId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class BufferIds(_message.Message):
    __slots__ = ("ids",)
    IDS_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, ids: _Optional[_Iterable[int]] = ...) -> None: ...

class BufferTime(_message.Message):
    __slots__ = ("device_id", "model_id", "timestamp", "tag")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    device_id: bytes
    model_id: bytes
    timestamp: int
    tag: int
    def __init__(self, device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., timestamp: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferEarlier(_message.Message):
    __slots__ = ("device_id", "model_id", "earlier", "tag")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    EARLIER_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    device_id: bytes
    model_id: bytes
    earlier: int
    tag: int
    def __init__(self, device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., earlier: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferLater(_message.Message):
    __slots__ = ("device_id", "model_id", "later", "tag")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    LATER_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    device_id: bytes
    model_id: bytes
    later: int
    tag: int
    def __init__(self, device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., later: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferRange(_message.Message):
    __slots__ = ("device_id", "model_id", "begin", "end", "tag")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    BEGIN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    device_id: bytes
    model_id: bytes
    begin: int
    end: int
    tag: int
    def __init__(self, device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., begin: _Optional[int] = ..., end: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferNumber(_message.Message):
    __slots__ = ("device_id", "model_id", "timestamp", "number", "tag")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    NUMBER_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    device_id: bytes
    model_id: bytes
    timestamp: int
    number: int
    tag: int
    def __init__(self, device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., timestamp: _Optional[int] = ..., number: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferSelector(_message.Message):
    __slots__ = ("device_id", "model_id", "tag")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    device_id: bytes
    model_id: bytes
    tag: int
    def __init__(self, device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., tag: _Optional[int] = ...) -> None: ...

class BuffersSelector(_message.Message):
    __slots__ = ("number", "offset", "device_id", "model_id", "tag")
    NUMBER_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    number: int
    offset: int
    device_id: bytes
    model_id: bytes
    tag: int
    def __init__(self, number: _Optional[int] = ..., offset: _Optional[int] = ..., device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferSetSchema(_message.Message):
    __slots__ = ("ids", "set_id", "timestamp", "data_bytes", "data_type", "tag")
    IDS_FIELD_NUMBER: _ClassVar[int]
    SET_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DATA_BYTES_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[int]
    set_id: bytes
    timestamp: int
    data_bytes: bytes
    data_type: _containers.RepeatedScalarFieldContainer[int]
    tag: int
    def __init__(self, ids: _Optional[_Iterable[int]] = ..., set_id: _Optional[bytes] = ..., timestamp: _Optional[int] = ..., data_bytes: _Optional[bytes] = ..., data_type: _Optional[_Iterable[int]] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferGroupTime(_message.Message):
    __slots__ = ("device_ids", "model_ids", "timestamp", "tag")
    DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    MODEL_IDS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    device_ids: _containers.RepeatedScalarFieldContainer[bytes]
    model_ids: _containers.RepeatedScalarFieldContainer[bytes]
    timestamp: int
    tag: int
    def __init__(self, device_ids: _Optional[_Iterable[bytes]] = ..., model_ids: _Optional[_Iterable[bytes]] = ..., timestamp: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferGroupEarlier(_message.Message):
    __slots__ = ("device_ids", "model_ids", "earlier", "tag")
    DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    MODEL_IDS_FIELD_NUMBER: _ClassVar[int]
    EARLIER_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    device_ids: _containers.RepeatedScalarFieldContainer[bytes]
    model_ids: _containers.RepeatedScalarFieldContainer[bytes]
    earlier: int
    tag: int
    def __init__(self, device_ids: _Optional[_Iterable[bytes]] = ..., model_ids: _Optional[_Iterable[bytes]] = ..., earlier: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferGroupLater(_message.Message):
    __slots__ = ("device_ids", "model_ids", "later", "tag")
    DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    MODEL_IDS_FIELD_NUMBER: _ClassVar[int]
    LATER_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    device_ids: _containers.RepeatedScalarFieldContainer[bytes]
    model_ids: _containers.RepeatedScalarFieldContainer[bytes]
    later: int
    tag: int
    def __init__(self, device_ids: _Optional[_Iterable[bytes]] = ..., model_ids: _Optional[_Iterable[bytes]] = ..., later: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferGroupRange(_message.Message):
    __slots__ = ("device_ids", "model_ids", "begin", "end", "tag")
    DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    MODEL_IDS_FIELD_NUMBER: _ClassVar[int]
    BEGIN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    device_ids: _containers.RepeatedScalarFieldContainer[bytes]
    model_ids: _containers.RepeatedScalarFieldContainer[bytes]
    begin: int
    end: int
    tag: int
    def __init__(self, device_ids: _Optional[_Iterable[bytes]] = ..., model_ids: _Optional[_Iterable[bytes]] = ..., begin: _Optional[int] = ..., end: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferGroupNumber(_message.Message):
    __slots__ = ("device_ids", "model_ids", "timestamp", "number", "tag")
    DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    MODEL_IDS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    NUMBER_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    device_ids: _containers.RepeatedScalarFieldContainer[bytes]
    model_ids: _containers.RepeatedScalarFieldContainer[bytes]
    timestamp: int
    number: int
    tag: int
    def __init__(self, device_ids: _Optional[_Iterable[bytes]] = ..., model_ids: _Optional[_Iterable[bytes]] = ..., timestamp: _Optional[int] = ..., number: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferGroupSelector(_message.Message):
    __slots__ = ("device_ids", "model_ids", "tag")
    DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    MODEL_IDS_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    device_ids: _containers.RepeatedScalarFieldContainer[bytes]
    model_ids: _containers.RepeatedScalarFieldContainer[bytes]
    tag: int
    def __init__(self, device_ids: _Optional[_Iterable[bytes]] = ..., model_ids: _Optional[_Iterable[bytes]] = ..., tag: _Optional[int] = ...) -> None: ...

class BuffersGroupSelector(_message.Message):
    __slots__ = ("number", "offset", "device_ids", "model_ids", "tag")
    NUMBER_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    MODEL_IDS_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    number: int
    offset: int
    device_ids: _containers.RepeatedScalarFieldContainer[bytes]
    model_ids: _containers.RepeatedScalarFieldContainer[bytes]
    tag: int
    def __init__(self, number: _Optional[int] = ..., offset: _Optional[int] = ..., device_ids: _Optional[_Iterable[bytes]] = ..., model_ids: _Optional[_Iterable[bytes]] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferSetTime(_message.Message):
    __slots__ = ("set_id", "timestamp", "tag")
    SET_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    set_id: bytes
    timestamp: int
    tag: int
    def __init__(self, set_id: _Optional[bytes] = ..., timestamp: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferSetEarlier(_message.Message):
    __slots__ = ("set_id", "earlier", "tag")
    SET_ID_FIELD_NUMBER: _ClassVar[int]
    EARLIER_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    set_id: bytes
    earlier: int
    tag: int
    def __init__(self, set_id: _Optional[bytes] = ..., earlier: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferSetLater(_message.Message):
    __slots__ = ("set_id", "later", "tag")
    SET_ID_FIELD_NUMBER: _ClassVar[int]
    LATER_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    set_id: bytes
    later: int
    tag: int
    def __init__(self, set_id: _Optional[bytes] = ..., later: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferSetRange(_message.Message):
    __slots__ = ("set_id", "begin", "end", "tag")
    SET_ID_FIELD_NUMBER: _ClassVar[int]
    BEGIN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    set_id: bytes
    begin: int
    end: int
    tag: int
    def __init__(self, set_id: _Optional[bytes] = ..., begin: _Optional[int] = ..., end: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferUpdate(_message.Message):
    __slots__ = ("id", "data_bytes", "data_type", "tag")
    ID_FIELD_NUMBER: _ClassVar[int]
    DATA_BYTES_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    id: int
    data_bytes: bytes
    data_type: _containers.RepeatedScalarFieldContainer[int]
    tag: int
    def __init__(self, id: _Optional[int] = ..., data_bytes: _Optional[bytes] = ..., data_type: _Optional[_Iterable[int]] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferUpdateTime(_message.Message):
    __slots__ = ("device_id", "model_id", "timestamp", "data_bytes", "data_type", "tag")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DATA_BYTES_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    device_id: bytes
    model_id: bytes
    timestamp: int
    data_bytes: bytes
    data_type: _containers.RepeatedScalarFieldContainer[int]
    tag: int
    def __init__(self, device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., timestamp: _Optional[int] = ..., data_bytes: _Optional[bytes] = ..., data_type: _Optional[_Iterable[int]] = ..., tag: _Optional[int] = ...) -> None: ...

class BufferReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: BufferSchema
    def __init__(self, result: _Optional[_Union[BufferSchema, _Mapping]] = ...) -> None: ...

class BufferListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[BufferSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[BufferSchema, _Mapping]]] = ...) -> None: ...

class BufferSetReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: BufferSetSchema
    def __init__(self, result: _Optional[_Union[BufferSetSchema, _Mapping]] = ...) -> None: ...

class BufferSetListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[BufferSetSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[BufferSetSchema, _Mapping]]] = ...) -> None: ...

class BufferCreateResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class BufferCreateMultipleResponse(_message.Message):
    __slots__ = ("ids",)
    IDS_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, ids: _Optional[_Iterable[int]] = ...) -> None: ...

class BufferChangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TimestampReadResponse(_message.Message):
    __slots__ = ("timestamp",)
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    timestamp: int
    def __init__(self, timestamp: _Optional[int] = ...) -> None: ...

class TimestampListResponse(_message.Message):
    __slots__ = ("timestamps",)
    TIMESTAMPS_FIELD_NUMBER: _ClassVar[int]
    timestamps: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, timestamps: _Optional[_Iterable[int]] = ...) -> None: ...

class BufferCountResponse(_message.Message):
    __slots__ = ("count",)
    COUNT_FIELD_NUMBER: _ClassVar[int]
    count: int
    def __init__(self, count: _Optional[int] = ...) -> None: ...
