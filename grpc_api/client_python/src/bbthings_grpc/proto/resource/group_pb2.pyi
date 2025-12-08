from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GroupModelSchema(_message.Message):
    __slots__ = ("id", "name", "category", "description", "model_ids")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    MODEL_IDS_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    name: str
    category: str
    description: str
    model_ids: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, id: _Optional[bytes] = ..., name: _Optional[str] = ..., category: _Optional[str] = ..., description: _Optional[str] = ..., model_ids: _Optional[_Iterable[bytes]] = ...) -> None: ...

class GroupDeviceSchema(_message.Message):
    __slots__ = ("id", "name", "category", "description", "device_ids")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    DEVICE_IDS_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    name: str
    category: str
    description: str
    device_ids: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, id: _Optional[bytes] = ..., name: _Optional[str] = ..., category: _Optional[str] = ..., description: _Optional[str] = ..., device_ids: _Optional[_Iterable[bytes]] = ...) -> None: ...

class GroupId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class GroupIds(_message.Message):
    __slots__ = ("ids",)
    IDS_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, ids: _Optional[_Iterable[bytes]] = ...) -> None: ...

class GroupName(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class GroupCategory(_message.Message):
    __slots__ = ("category",)
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    category: str
    def __init__(self, category: _Optional[str] = ...) -> None: ...

class GroupOption(_message.Message):
    __slots__ = ("name", "category")
    NAME_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    name: str
    category: str
    def __init__(self, name: _Optional[str] = ..., category: _Optional[str] = ...) -> None: ...

class GroupUpdate(_message.Message):
    __slots__ = ("id", "name", "category", "description")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    name: str
    category: str
    description: str
    def __init__(self, id: _Optional[bytes] = ..., name: _Optional[str] = ..., category: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class GroupModel(_message.Message):
    __slots__ = ("id", "model_id")
    ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    model_id: bytes
    def __init__(self, id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ...) -> None: ...

class GroupDevice(_message.Message):
    __slots__ = ("id", "device_id")
    ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    device_id: bytes
    def __init__(self, id: _Optional[bytes] = ..., device_id: _Optional[bytes] = ...) -> None: ...

class GroupModelReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: GroupModelSchema
    def __init__(self, result: _Optional[_Union[GroupModelSchema, _Mapping]] = ...) -> None: ...

class GroupModelListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[GroupModelSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[GroupModelSchema, _Mapping]]] = ...) -> None: ...

class GroupDeviceReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: GroupDeviceSchema
    def __init__(self, result: _Optional[_Union[GroupDeviceSchema, _Mapping]] = ...) -> None: ...

class GroupDeviceListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[GroupDeviceSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[GroupDeviceSchema, _Mapping]]] = ...) -> None: ...

class GroupCreateResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class GroupChangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
