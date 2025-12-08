from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ModelSchema(_message.Message):
    __slots__ = ("id", "category", "name", "description", "data_type", "tags", "configs")
    ID_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    CONFIGS_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    category: str
    name: str
    description: str
    data_type: _containers.RepeatedScalarFieldContainer[int]
    tags: _containers.RepeatedCompositeFieldContainer[TagSchema]
    configs: _containers.RepeatedCompositeFieldContainer[ConfigSchemaVec]
    def __init__(self, id: _Optional[bytes] = ..., category: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., data_type: _Optional[_Iterable[int]] = ..., tags: _Optional[_Iterable[_Union[TagSchema, _Mapping]]] = ..., configs: _Optional[_Iterable[_Union[ConfigSchemaVec, _Mapping]]] = ...) -> None: ...

class ConfigSchemaVec(_message.Message):
    __slots__ = ("configs",)
    CONFIGS_FIELD_NUMBER: _ClassVar[int]
    configs: _containers.RepeatedCompositeFieldContainer[ConfigSchema]
    def __init__(self, configs: _Optional[_Iterable[_Union[ConfigSchema, _Mapping]]] = ...) -> None: ...

class ModelId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class ModelIds(_message.Message):
    __slots__ = ("ids",)
    IDS_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, ids: _Optional[_Iterable[bytes]] = ...) -> None: ...

class TypeId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class ModelName(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class ModelCategory(_message.Message):
    __slots__ = ("category",)
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    category: str
    def __init__(self, category: _Optional[str] = ...) -> None: ...

class ModelOption(_message.Message):
    __slots__ = ("type_id", "name", "category")
    TYPE_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    type_id: bytes
    name: str
    category: str
    def __init__(self, type_id: _Optional[bytes] = ..., name: _Optional[str] = ..., category: _Optional[str] = ...) -> None: ...

class ModelUpdate(_message.Message):
    __slots__ = ("id", "category", "name", "description", "data_type", "data_type_flag")
    ID_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FLAG_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    category: str
    name: str
    description: str
    data_type: _containers.RepeatedScalarFieldContainer[int]
    data_type_flag: bool
    def __init__(self, id: _Optional[bytes] = ..., category: _Optional[str] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., data_type: _Optional[_Iterable[int]] = ..., data_type_flag: bool = ...) -> None: ...

class ConfigSchema(_message.Message):
    __slots__ = ("id", "model_id", "index", "name", "config_bytes", "config_type", "category")
    ID_FIELD_NUMBER: _ClassVar[int]
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONFIG_BYTES_FIELD_NUMBER: _ClassVar[int]
    CONFIG_TYPE_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    id: int
    model_id: bytes
    index: int
    name: str
    config_bytes: bytes
    config_type: int
    category: str
    def __init__(self, id: _Optional[int] = ..., model_id: _Optional[bytes] = ..., index: _Optional[int] = ..., name: _Optional[str] = ..., config_bytes: _Optional[bytes] = ..., config_type: _Optional[int] = ..., category: _Optional[str] = ...) -> None: ...

class ConfigId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class ConfigUpdate(_message.Message):
    __slots__ = ("id", "name", "config_bytes", "config_type", "category")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    CONFIG_BYTES_FIELD_NUMBER: _ClassVar[int]
    CONFIG_TYPE_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    config_bytes: bytes
    config_type: int
    category: str
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., config_bytes: _Optional[bytes] = ..., config_type: _Optional[int] = ..., category: _Optional[str] = ...) -> None: ...

class TagSchema(_message.Message):
    __slots__ = ("model_id", "tag", "name", "members")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    model_id: bytes
    tag: int
    name: str
    members: _containers.RepeatedScalarFieldContainer[int]
    def __init__(self, model_id: _Optional[bytes] = ..., tag: _Optional[int] = ..., name: _Optional[str] = ..., members: _Optional[_Iterable[int]] = ...) -> None: ...

class TagId(_message.Message):
    __slots__ = ("model_id", "tag")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    model_id: bytes
    tag: int
    def __init__(self, model_id: _Optional[bytes] = ..., tag: _Optional[int] = ...) -> None: ...

class TagUpdate(_message.Message):
    __slots__ = ("model_id", "tag", "name", "members", "members_flag")
    MODEL_ID_FIELD_NUMBER: _ClassVar[int]
    TAG_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    MEMBERS_FLAG_FIELD_NUMBER: _ClassVar[int]
    model_id: bytes
    tag: int
    name: str
    members: _containers.RepeatedScalarFieldContainer[int]
    members_flag: bool
    def __init__(self, model_id: _Optional[bytes] = ..., tag: _Optional[int] = ..., name: _Optional[str] = ..., members: _Optional[_Iterable[int]] = ..., members_flag: bool = ...) -> None: ...

class ModelReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: ModelSchema
    def __init__(self, result: _Optional[_Union[ModelSchema, _Mapping]] = ...) -> None: ...

class ModelListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[ModelSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[ModelSchema, _Mapping]]] = ...) -> None: ...

class ModelCreateResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class ModelChangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ConfigReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: ConfigSchema
    def __init__(self, result: _Optional[_Union[ConfigSchema, _Mapping]] = ...) -> None: ...

class ConfigListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[ConfigSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[ConfigSchema, _Mapping]]] = ...) -> None: ...

class ConfigCreateResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class ConfigChangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class TagReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: TagSchema
    def __init__(self, result: _Optional[_Union[TagSchema, _Mapping]] = ...) -> None: ...

class TagListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[TagSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[TagSchema, _Mapping]]] = ...) -> None: ...

class TagChangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
