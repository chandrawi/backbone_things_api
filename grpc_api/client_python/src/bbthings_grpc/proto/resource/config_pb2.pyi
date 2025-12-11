from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ApiIdRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class AccessRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ApiIdResponse(_message.Message):
    __slots__ = ("api_id",)
    API_ID_FIELD_NUMBER: _ClassVar[int]
    api_id: bytes
    def __init__(self, api_id: _Optional[bytes] = ...) -> None: ...

class ProcedureAcces(_message.Message):
    __slots__ = ("procedure", "roles")
    PROCEDURE_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    procedure: str
    roles: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, procedure: _Optional[str] = ..., roles: _Optional[_Iterable[str]] = ...) -> None: ...

class RoleAcces(_message.Message):
    __slots__ = ("role", "procedures")
    ROLE_FIELD_NUMBER: _ClassVar[int]
    PROCEDURES_FIELD_NUMBER: _ClassVar[int]
    role: str
    procedures: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, role: _Optional[str] = ..., procedures: _Optional[_Iterable[str]] = ...) -> None: ...

class ProcedureAccesResponse(_message.Message):
    __slots__ = ("access",)
    ACCESS_FIELD_NUMBER: _ClassVar[int]
    access: _containers.RepeatedCompositeFieldContainer[ProcedureAcces]
    def __init__(self, access: _Optional[_Iterable[_Union[ProcedureAcces, _Mapping]]] = ...) -> None: ...

class RoleAccesResponse(_message.Message):
    __slots__ = ("access",)
    ACCESS_FIELD_NUMBER: _ClassVar[int]
    access: _containers.RepeatedCompositeFieldContainer[RoleAcces]
    def __init__(self, access: _Optional[_Iterable[_Union[RoleAcces, _Mapping]]] = ...) -> None: ...
