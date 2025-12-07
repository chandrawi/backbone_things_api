from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class DataSchema(_message.Message):
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

class DataMultipleSchema(_message.Message):
    __slots__ = ("schemas",)
    SCHEMAS_FIELD_NUMBER: _ClassVar[int]
    schemas: _containers.RepeatedCompositeFieldContainer[DataSchema]
    def __init__(self, schemas: _Optional[_Iterable[_Union[DataSchema, _Mapping]]] = ...) -> None: ...

class DataTime(_message.Message):
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

class DataEarlier(_message.Message):
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

class DataLater(_message.Message):
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

class DataRange(_message.Message):
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

class DataNumber(_message.Message):
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

class DataSetSchema(_message.Message):
    __slots__ = ("set_id", "timestamp", "data_bytes", "data_type", "tag")
    SET_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    DATA_BYTES_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    set_id: bytes
    timestamp: int
    data_bytes: bytes
    data_type: _containers.RepeatedScalarFieldContainer[int]
    tag: int
    def __init__(self, set_id: _Optional[bytes] = ..., timestamp: _Optional[int] = ..., data_bytes: _Optional[bytes] = ..., data_type: _Optional[_Iterable[int]] = ..., tag: _Optional[int] = ...) -> None: ...

class DataGroupTime(_message.Message):
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

class DataGroupEarlier(_message.Message):
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

class DataGroupLater(_message.Message):
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

class DataGroupRange(_message.Message):
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

class DataGroupNumber(_message.Message):
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

class DataSetTime(_message.Message):
    __slots__ = ("set_id", "timestamp", "tag")
    SET_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    set_id: bytes
    timestamp: int
    tag: int
    def __init__(self, set_id: _Optional[bytes] = ..., timestamp: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class DataSetEarlier(_message.Message):
    __slots__ = ("set_id", "earlier", "tag")
    SET_ID_FIELD_NUMBER: _ClassVar[int]
    EARLIER_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    set_id: bytes
    earlier: int
    tag: int
    def __init__(self, set_id: _Optional[bytes] = ..., earlier: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class DataSetLater(_message.Message):
    __slots__ = ("set_id", "later", "tag")
    SET_ID_FIELD_NUMBER: _ClassVar[int]
    LATER_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    set_id: bytes
    later: int
    tag: int
    def __init__(self, set_id: _Optional[bytes] = ..., later: _Optional[int] = ..., tag: _Optional[int] = ...) -> None: ...

class DataSetRange(_message.Message):
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

class DataReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: DataSchema
    def __init__(self, result: _Optional[_Union[DataSchema, _Mapping]] = ...) -> None: ...

class DataListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[DataSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[DataSchema, _Mapping]]] = ...) -> None: ...

class DataSetReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: DataSetSchema
    def __init__(self, result: _Optional[_Union[DataSetSchema, _Mapping]] = ...) -> None: ...

class DataSetListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[DataSetSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[DataSetSchema, _Mapping]]] = ...) -> None: ...

class DataChangeResponse(_message.Message):
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

class DataCountResponse(_message.Message):
    __slots__ = ("count",)
    COUNT_FIELD_NUMBER: _ClassVar[int]
    count: int
    def __init__(self, count: _Optional[int] = ...) -> None: ...
