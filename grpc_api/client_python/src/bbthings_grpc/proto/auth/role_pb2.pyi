from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RoleSchema(_message.Message):
    __slots__ = ("id", "api_id", "name", "multi", "ip_lock", "access_duration", "refresh_duration", "access_key", "procedures")
    ID_FIELD_NUMBER: _ClassVar[int]
    API_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    MULTI_FIELD_NUMBER: _ClassVar[int]
    IP_LOCK_FIELD_NUMBER: _ClassVar[int]
    ACCESS_DURATION_FIELD_NUMBER: _ClassVar[int]
    REFRESH_DURATION_FIELD_NUMBER: _ClassVar[int]
    ACCESS_KEY_FIELD_NUMBER: _ClassVar[int]
    PROCEDURES_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    api_id: bytes
    name: str
    multi: bool
    ip_lock: bool
    access_duration: int
    refresh_duration: int
    access_key: bytes
    procedures: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, id: _Optional[bytes] = ..., api_id: _Optional[bytes] = ..., name: _Optional[str] = ..., multi: bool = ..., ip_lock: bool = ..., access_duration: _Optional[int] = ..., refresh_duration: _Optional[int] = ..., access_key: _Optional[bytes] = ..., procedures: _Optional[_Iterable[bytes]] = ...) -> None: ...

class RoleId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class RoleIds(_message.Message):
    __slots__ = ("ids",)
    IDS_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, ids: _Optional[_Iterable[bytes]] = ...) -> None: ...

class RoleName(_message.Message):
    __slots__ = ("api_id", "name")
    API_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    api_id: bytes
    name: str
    def __init__(self, api_id: _Optional[bytes] = ..., name: _Optional[str] = ...) -> None: ...

class ApiId(_message.Message):
    __slots__ = ("api_id",)
    API_ID_FIELD_NUMBER: _ClassVar[int]
    api_id: bytes
    def __init__(self, api_id: _Optional[bytes] = ...) -> None: ...

class UserId(_message.Message):
    __slots__ = ("user_id",)
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    user_id: bytes
    def __init__(self, user_id: _Optional[bytes] = ...) -> None: ...

class RoleOption(_message.Message):
    __slots__ = ("api_id", "user_id", "name")
    API_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    api_id: bytes
    user_id: bytes
    name: str
    def __init__(self, api_id: _Optional[bytes] = ..., user_id: _Optional[bytes] = ..., name: _Optional[str] = ...) -> None: ...

class RoleUpdate(_message.Message):
    __slots__ = ("id", "name", "multi", "ip_lock", "access_duration", "refresh_duration")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    MULTI_FIELD_NUMBER: _ClassVar[int]
    IP_LOCK_FIELD_NUMBER: _ClassVar[int]
    ACCESS_DURATION_FIELD_NUMBER: _ClassVar[int]
    REFRESH_DURATION_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    name: str
    multi: bool
    ip_lock: bool
    access_duration: int
    refresh_duration: int
    def __init__(self, id: _Optional[bytes] = ..., name: _Optional[str] = ..., multi: bool = ..., ip_lock: bool = ..., access_duration: _Optional[int] = ..., refresh_duration: _Optional[int] = ...) -> None: ...

class RoleAccess(_message.Message):
    __slots__ = ("id", "procedure_id")
    ID_FIELD_NUMBER: _ClassVar[int]
    PROCEDURE_ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    procedure_id: bytes
    def __init__(self, id: _Optional[bytes] = ..., procedure_id: _Optional[bytes] = ...) -> None: ...

class RoleReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: RoleSchema
    def __init__(self, result: _Optional[_Union[RoleSchema, _Mapping]] = ...) -> None: ...

class RoleListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[RoleSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[RoleSchema, _Mapping]]] = ...) -> None: ...

class RoleCreateResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class RoleChangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
