from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class UserRoleSchema(_message.Message):
    __slots__ = ("api_id", "role", "multi", "ip_lock", "access_duration", "refresh_duration", "access_key")
    API_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    MULTI_FIELD_NUMBER: _ClassVar[int]
    IP_LOCK_FIELD_NUMBER: _ClassVar[int]
    ACCESS_DURATION_FIELD_NUMBER: _ClassVar[int]
    REFRESH_DURATION_FIELD_NUMBER: _ClassVar[int]
    ACCESS_KEY_FIELD_NUMBER: _ClassVar[int]
    api_id: bytes
    role: str
    multi: bool
    ip_lock: bool
    access_duration: int
    refresh_duration: int
    access_key: bytes
    def __init__(self, api_id: _Optional[bytes] = ..., role: _Optional[str] = ..., multi: bool = ..., ip_lock: bool = ..., access_duration: _Optional[int] = ..., refresh_duration: _Optional[int] = ..., access_key: _Optional[bytes] = ...) -> None: ...

class UserSchema(_message.Message):
    __slots__ = ("id", "name", "email", "phone", "password", "roles")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    PHONE_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    name: str
    email: str
    phone: str
    password: str
    roles: _containers.RepeatedCompositeFieldContainer[UserRoleSchema]
    def __init__(self, id: _Optional[bytes] = ..., name: _Optional[str] = ..., email: _Optional[str] = ..., phone: _Optional[str] = ..., password: _Optional[str] = ..., roles: _Optional[_Iterable[_Union[UserRoleSchema, _Mapping]]] = ...) -> None: ...

class UserId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class UserIds(_message.Message):
    __slots__ = ("ids",)
    IDS_FIELD_NUMBER: _ClassVar[int]
    ids: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, ids: _Optional[_Iterable[bytes]] = ...) -> None: ...

class UserName(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class ApiId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class RoleId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class UserOption(_message.Message):
    __slots__ = ("api_id", "role_id", "name")
    API_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    api_id: bytes
    role_id: bytes
    name: str
    def __init__(self, api_id: _Optional[bytes] = ..., role_id: _Optional[bytes] = ..., name: _Optional[str] = ...) -> None: ...

class UserUpdate(_message.Message):
    __slots__ = ("id", "name", "email", "phone", "password")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    EMAIL_FIELD_NUMBER: _ClassVar[int]
    PHONE_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    name: str
    email: str
    phone: str
    password: str
    def __init__(self, id: _Optional[bytes] = ..., name: _Optional[str] = ..., email: _Optional[str] = ..., phone: _Optional[str] = ..., password: _Optional[str] = ...) -> None: ...

class UserRole(_message.Message):
    __slots__ = ("user_id", "role_id")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    user_id: bytes
    role_id: bytes
    def __init__(self, user_id: _Optional[bytes] = ..., role_id: _Optional[bytes] = ...) -> None: ...

class UserReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: UserSchema
    def __init__(self, result: _Optional[_Union[UserSchema, _Mapping]] = ...) -> None: ...

class UserListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[UserSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[UserSchema, _Mapping]]] = ...) -> None: ...

class UserCreateResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class UserChangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
