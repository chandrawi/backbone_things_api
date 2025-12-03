from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SliceSchema(_message.Message):
    __slots__ = ("id", "device_id", "model_id", "timestamp_begin", "timestamp_end", "name", "description")
    ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_BEGIN_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_END_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    id: int
    device_id: bytes
    model_id: bytes
    timestamp_begin: int
    timestamp_end: int
    name: str
    description: str
    def __init__(self, id: _Optional[int] = ..., device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., timestamp_begin: _Optional[int] = ..., timestamp_end: _Optional[int] = ..., name: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class SliceId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class SliceIds(_message.Message):
    __slots__ = ("ids",)
    IDS_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, ids: _Optional[_Iterable[int]] = ...) -> None: ...

class SliceTime(_message.Message):
    __slots__ = ("device_id", "model_id", "timestamp")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    device_id: bytes
    model_id: bytes
    timestamp: int
    def __init__(self, device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., timestamp: _Optional[int] = ...) -> None: ...

class SliceRange(_message.Message):
    __slots__ = ("device_id", "model_id", "begin", "end")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    BEGIN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    device_id: bytes
    model_id: bytes
    begin: int
    end: int
    def __init__(self, device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., begin: _Optional[int] = ..., end: _Optional[int] = ...) -> None: ...

class SliceNameTime(_message.Message):
    __slots__ = ("name", "timestamp")
    NAME_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    name: str
    timestamp: int
    def __init__(self, name: _Optional[str] = ..., timestamp: _Optional[int] = ...) -> None: ...

class SliceNameRange(_message.Message):
    __slots__ = ("name", "begin", "end")
    NAME_FIELD_NUMBER: _ClassVar[int]
    BEGIN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    name: str
    begin: int
    end: int
    def __init__(self, name: _Optional[str] = ..., begin: _Optional[int] = ..., end: _Optional[int] = ...) -> None: ...

class SliceOption(_message.Message):
    __slots__ = ("device_id", "model_id", "name", "begin", "end")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    BEGIN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    device_id: bytes
    model_id: bytes
    name: str
    begin: int
    end: int
    def __init__(self, device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., name: _Optional[str] = ..., begin: _Optional[int] = ..., end: _Optional[int] = ...) -> None: ...

class SliceGroupTime(_message.Message):
    __slots__ = ("device_ids", "model_ids", "timestamp")
    DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    MODEL_IDS_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    device_ids: _containers.RepeatedScalarFieldContainer[bytes]
    model_ids: _containers.RepeatedScalarFieldContainer[bytes]
    timestamp: int
    def __init__(self, device_ids: _Optional[_Iterable[bytes]] = ..., model_ids: _Optional[_Iterable[bytes]] = ..., timestamp: _Optional[int] = ...) -> None: ...

class SliceGroupRange(_message.Message):
    __slots__ = ("device_ids", "model_ids", "begin", "end")
    DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    MODEL_IDS_FIELD_NUMBER: _ClassVar[int]
    BEGIN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    device_ids: _containers.RepeatedScalarFieldContainer[bytes]
    model_ids: _containers.RepeatedScalarFieldContainer[bytes]
    begin: int
    end: int
    def __init__(self, device_ids: _Optional[_Iterable[bytes]] = ..., model_ids: _Optional[_Iterable[bytes]] = ..., begin: _Optional[int] = ..., end: _Optional[int] = ...) -> None: ...

class SliceGroupOption(_message.Message):
    __slots__ = ("device_ids", "model_ids", "name", "begin", "end")
    DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    MODEL_IDS_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    BEGIN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    device_ids: _containers.RepeatedScalarFieldContainer[bytes]
    model_ids: _containers.RepeatedScalarFieldContainer[bytes]
    name: str
    begin: int
    end: int
    def __init__(self, device_ids: _Optional[_Iterable[bytes]] = ..., model_ids: _Optional[_Iterable[bytes]] = ..., name: _Optional[str] = ..., begin: _Optional[int] = ..., end: _Optional[int] = ...) -> None: ...

class SliceUpdate(_message.Message):
    __slots__ = ("id", "timestamp_begin", "timestamp_end", "name", "description")
    ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_BEGIN_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_END_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    id: int
    timestamp_begin: int
    timestamp_end: int
    name: str
    description: str
    def __init__(self, id: _Optional[int] = ..., timestamp_begin: _Optional[int] = ..., timestamp_end: _Optional[int] = ..., name: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class SliceSetSchema(_message.Message):
    __slots__ = ("id", "set_id", "timestamp_begin", "timestamp_end", "name", "description")
    ID_FIELD_NUMBER: _ClassVar[int]
    SET_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_BEGIN_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_END_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    id: int
    set_id: bytes
    timestamp_begin: int
    timestamp_end: int
    name: str
    description: str
    def __init__(self, id: _Optional[int] = ..., set_id: _Optional[bytes] = ..., timestamp_begin: _Optional[int] = ..., timestamp_end: _Optional[int] = ..., name: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class SliceSetTime(_message.Message):
    __slots__ = ("set_id", "timestamp")
    SET_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    set_id: bytes
    timestamp: int
    def __init__(self, set_id: _Optional[bytes] = ..., timestamp: _Optional[int] = ...) -> None: ...

class SliceSetRange(_message.Message):
    __slots__ = ("set_id", "begin", "end")
    SET_ID_FIELD_NUMBER: _ClassVar[int]
    BEGIN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    set_id: bytes
    begin: int
    end: int
    def __init__(self, set_id: _Optional[bytes] = ..., begin: _Optional[int] = ..., end: _Optional[int] = ...) -> None: ...

class SliceSetOption(_message.Message):
    __slots__ = ("set_id", "name", "begin", "end")
    SET_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    BEGIN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    set_id: bytes
    name: str
    begin: int
    end: int
    def __init__(self, set_id: _Optional[bytes] = ..., name: _Optional[str] = ..., begin: _Optional[int] = ..., end: _Optional[int] = ...) -> None: ...

class SliceReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: SliceSchema
    def __init__(self, result: _Optional[_Union[SliceSchema, _Mapping]] = ...) -> None: ...

class SliceListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[SliceSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[SliceSchema, _Mapping]]] = ...) -> None: ...

class SliceCreateResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class SliceChangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SliceSetReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: SliceSetSchema
    def __init__(self, result: _Optional[_Union[SliceSetSchema, _Mapping]] = ...) -> None: ...

class SliceSetListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[SliceSetSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[SliceSetSchema, _Mapping]]] = ...) -> None: ...
