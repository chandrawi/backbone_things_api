from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ApiSchema(_message.Message):
    __slots__ = ("id", "name", "address", "category", "description", "password", "access_key", "procedures")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    ACCESS_KEY_FIELD_NUMBER: _ClassVar[int]
    PROCEDURES_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    name: str
    address: str
    category: str
    description: str
    password: str
    access_key: bytes
    procedures: _containers.RepeatedCompositeFieldContainer[ProcedureSchema]
    def __init__(self, id: _Optional[bytes] = ..., name: _Optional[str] = ..., address: _Optional[str] = ..., category: _Optional[str] = ..., description: _Optional[str] = ..., password: _Optional[str] = ..., access_key: _Optional[bytes] = ..., procedures: _Optional[_Iterable[_Union[ProcedureSchema, _Mapping]]] = ...) -> None: ...

class ApiId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class ApiIds(_message.Message):
    __slots__ = ("ids",)
    IDS_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, ids: _Optional[_Iterable[bytes]] = ...) -> None: ...

class ApiName(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class ApiCategory(_message.Message):
    __slots__ = ("category",)
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    category: str
    def __init__(self, category: _Optional[str] = ...) -> None: ...

class ApiOption(_message.Message):
    __slots__ = ("name", "category")
    NAME_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    name: str
    category: str
    def __init__(self, name: _Optional[str] = ..., category: _Optional[str] = ...) -> None: ...

class ApiUpdate(_message.Message):
    __slots__ = ("id", "name", "address", "category", "description", "password", "access_key")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    ACCESS_KEY_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    name: str
    address: str
    category: str
    description: str
    password: str
    access_key: bytes
    def __init__(self, id: _Optional[bytes] = ..., name: _Optional[str] = ..., address: _Optional[str] = ..., category: _Optional[str] = ..., description: _Optional[str] = ..., password: _Optional[str] = ..., access_key: _Optional[bytes] = ...) -> None: ...

class ProcedureSchema(_message.Message):
    __slots__ = ("id", "api_id", "name", "description", "roles")
    ID_FIELD_NUMBER: _ClassVar[int]
    API_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    api_id: bytes
    name: str
    description: str
    roles: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id: _Optional[bytes] = ..., api_id: _Optional[bytes] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., roles: _Optional[_Iterable[str]] = ...) -> None: ...

class ProcedureId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class ProcedureIds(_message.Message):
    __slots__ = ("ids",)
    IDS_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, ids: _Optional[_Iterable[bytes]] = ...) -> None: ...

class ProcedureName(_message.Message):
    __slots__ = ("api_id", "name")
    API_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    api_id: bytes
    name: str
    def __init__(self, api_id: _Optional[bytes] = ..., name: _Optional[str] = ...) -> None: ...

class ProcedureOption(_message.Message):
    __slots__ = ("api_id", "name")
    API_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    api_id: bytes
    name: str
    def __init__(self, api_id: _Optional[bytes] = ..., name: _Optional[str] = ...) -> None: ...

class ProcedureUpdate(_message.Message):
    __slots__ = ("id", "name", "description")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    name: str
    description: str
    def __init__(self, id: _Optional[bytes] = ..., name: _Optional[str] = ..., description: _Optional[str] = ...) -> None: ...

class ApiReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: ApiSchema
    def __init__(self, result: _Optional[_Union[ApiSchema, _Mapping]] = ...) -> None: ...

class ApiListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[ApiSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[ApiSchema, _Mapping]]] = ...) -> None: ...

class ApiCreateResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class ApiChangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ProcedureReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: ProcedureSchema
    def __init__(self, result: _Optional[_Union[ProcedureSchema, _Mapping]] = ...) -> None: ...

class ProcedureListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[ProcedureSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[ProcedureSchema, _Mapping]]] = ...) -> None: ...

class ProcedureCreateResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class ProcedureChangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
