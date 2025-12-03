from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SetSchema(_message.Message):
    __slots__ = ("id", "template_id", "name", "description", "members")
    ID_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    template_id: bytes
    name: str
    description: str
    members: _containers.RepeatedCompositeFieldContainer[SetMember]
    def __init__(self, id: _Optional[bytes] = ..., template_id: _Optional[bytes] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., members: _Optional[_Iterable[_Union[SetMember, _Mapping]]] = ...) -> None: ...

class SetMember(_message.Message):
    __slots__ = ("device_id", "model_id", "data_index")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_INDEX_FIELD_NUMBER: _ClassVar[int]
    device_id: bytes
    model_id: bytes
    data_index: bytes
    def __init__(self, device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., data_index: _Optional[bytes] = ...) -> None: ...

class SetId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class SetIds(_message.Message):
    __slots__ = ("ids",)
    IDS_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, ids: _Optional[_Iterable[bytes]] = ...) -> None: ...

class SetName(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class SetOption(_message.Message):
    __slots__ = ("template_id", "name")
    TEMPLATE_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    template_id: bytes
    name: str
    def __init__(self, template_id: _Optional[bytes] = ..., name: _Optional[str] = ...) -> None: ...

class SetUpdate(_message.Message):
    __slots__ = ("id", "template_id", "name", "description")
    ID_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    template_id: bytes
    name: str
    description: str
    def __init__(self, id: _Optional[bytes] = ..., template_id: _Optional[bytes] = ..., name: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class SetMemberRequest(_message.Message):
    __slots__ = ("id", "device_id", "model_id", "data_index")
    ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_INDEX_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    device_id: bytes
    model_id: bytes
    data_index: bytes
    def __init__(self, id: _Optional[bytes] = ..., device_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., data_index: _Optional[bytes] = ...) -> None: ...

class SetMemberSwap(_message.Message):
    __slots__ = ("id", "device_id_1", "model_id_1", "device_id_2", "model_id_2")
    ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_1_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_1_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_2_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_2_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    device_id_1: bytes
    model_id_1: bytes
    device_id_2: bytes
    model_id_2: bytes
    def __init__(self, id: _Optional[bytes] = ..., device_id_1: _Optional[bytes] = ..., model_id_1: _Optional[bytes] = ..., device_id_2: _Optional[bytes] = ..., model_id_2: _Optional[bytes] = ...) -> None: ...

class SetTemplateSchema(_message.Message):
    __slots__ = ("id", "name", "description", "members")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    name: str
    description: str
    members: _containers.RepeatedCompositeFieldContainer[SetTemplateMember]
    def __init__(self, id: _Optional[bytes] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., members: _Optional[_Iterable[_Union[SetTemplateMember, _Mapping]]] = ...) -> None: ...

class SetTemplateMember(_message.Message):
    __slots__ = ("type_id", "model_id", "data_index")
    TYPE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_INDEX_FIELD_NUMBER: _ClassVar[int]
    type_id: bytes
    model_id: bytes
    data_index: bytes
    def __init__(self, type_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., data_index: _Optional[bytes] = ...) -> None: ...

class SetTemplateId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class SetTemplateIds(_message.Message):
    __slots__ = ("ids",)
    IDS_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, ids: _Optional[_Iterable[bytes]] = ...) -> None: ...

class SetTemplateName(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class SetTemplateOption(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class SetTemplateUpdate(_message.Message):
    __slots__ = ("id", "name", "description")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    name: str
    description: str
    def __init__(self, id: _Optional[bytes] = ..., name: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class SetTemplateMemberRequest(_message.Message):
    __slots__ = ("id", "type_id", "model_id", "data_index", "template_index")
    ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_INDEX_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_INDEX_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    type_id: bytes
    model_id: bytes
    data_index: bytes
    template_index: int
    def __init__(self, id: _Optional[bytes] = ..., type_id: _Optional[bytes] = ..., model_id: _Optional[bytes] = ..., data_index: _Optional[bytes] = ..., template_index: _Optional[int] = ...) -> None: ...

class SetTemplateMemberSwap(_message.Message):
    __slots__ = ("id", "template_index_1", "template_index_2")
    ID_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_INDEX_1_FIELD_NUMBER: _ClassVar[int]
    TEMPLATE_INDEX_2_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    template_index_1: int
    template_index_2: int
    def __init__(self, id: _Optional[bytes] = ..., template_index_1: _Optional[int] = ..., template_index_2: _Optional[int] = ...) -> None: ...

class SetReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: SetSchema
    def __init__(self, result: _Optional[_Union[SetSchema, _Mapping]] = ...) -> None: ...

class SetListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[SetSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[SetSchema, _Mapping]]] = ...) -> None: ...

class SetCreateResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class SetChangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TemplateReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: SetTemplateSchema
    def __init__(self, result: _Optional[_Union[SetTemplateSchema, _Mapping]] = ...) -> None: ...

class TemplateListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[SetTemplateSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[SetTemplateSchema, _Mapping]]] = ...) -> None: ...

class TemplateCreateResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class TemplateChangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
