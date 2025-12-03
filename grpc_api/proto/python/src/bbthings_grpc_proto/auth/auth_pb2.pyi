from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ApiKeyRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ApiKeyResponse(_message.Message):
    __slots__ = ("public_key",)
    PUBLIC_KEY_FIELD_NUMBER: _ClassVar[int]
    public_key: bytes
    def __init__(self, public_key: _Optional[bytes] = ...) -> None: ...

class ApiLoginRequest(_message.Message):
    __slots__ = ("api_id", "password", "public_key")
    API_ID_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    PUBLIC_KEY_FIELD_NUMBER: _ClassVar[int]
    api_id: bytes
    password: bytes
    public_key: bytes
    def __init__(self, api_id: _Optional[bytes] = ..., password: _Optional[bytes] = ..., public_key: _Optional[bytes] = ...) -> None: ...

class ProcedureMap(_message.Message):
    __slots__ = ("procedure", "roles")
    PROCEDURE_FIELD_NUMBER: _ClassVar[int]
    ROLES_FIELD_NUMBER: _ClassVar[int]
    procedure: str
    roles: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, procedure: _Optional[str] = ..., roles: _Optional[_Iterable[str]] = ...) -> None: ...

class ApiLoginResponse(_message.Message):
    __slots__ = ("root_key", "access_key", "access_procedures")
    ROOT_KEY_FIELD_NUMBER: _ClassVar[int]
    ACCESS_KEY_FIELD_NUMBER: _ClassVar[int]
    ACCESS_PROCEDURES_FIELD_NUMBER: _ClassVar[int]
    root_key: bytes
    access_key: bytes
    access_procedures: _containers.RepeatedCompositeFieldContainer[ProcedureMap]
    def __init__(self, root_key: _Optional[bytes] = ..., access_key: _Optional[bytes] = ..., access_procedures: _Optional[_Iterable[_Union[ProcedureMap, _Mapping]]] = ...) -> None: ...

class UserKeyRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UserKeyResponse(_message.Message):
    __slots__ = ("public_key",)
    PUBLIC_KEY_FIELD_NUMBER: _ClassVar[int]
    public_key: bytes
    def __init__(self, public_key: _Optional[bytes] = ...) -> None: ...

class UserLoginRequest(_message.Message):
    __slots__ = ("username", "password")
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    username: str
    password: bytes
    def __init__(self, username: _Optional[str] = ..., password: _Optional[bytes] = ...) -> None: ...

class AccessTokenMap(_message.Message):
    __slots__ = ("api_id", "access_token", "refresh_token")
    API_ID_FIELD_NUMBER: _ClassVar[int]
    ACCESS_TOKEN_FIELD_NUMBER: _ClassVar[int]
    REFRESH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    api_id: bytes
    access_token: str
    refresh_token: str
    def __init__(self, api_id: _Optional[bytes] = ..., access_token: _Optional[str] = ..., refresh_token: _Optional[str] = ...) -> None: ...

class UserLoginResponse(_message.Message):
    __slots__ = ("user_id", "auth_token", "access_tokens")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    AUTH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    ACCESS_TOKENS_FIELD_NUMBER: _ClassVar[int]
    user_id: bytes
    auth_token: str
    access_tokens: _containers.RepeatedCompositeFieldContainer[AccessTokenMap]
    def __init__(self, user_id: _Optional[bytes] = ..., auth_token: _Optional[str] = ..., access_tokens: _Optional[_Iterable[_Union[AccessTokenMap, _Mapping]]] = ...) -> None: ...

class UserRefreshRequest(_message.Message):
    __slots__ = ("api_id", "access_token", "refresh_token")
    API_ID_FIELD_NUMBER: _ClassVar[int]
    ACCESS_TOKEN_FIELD_NUMBER: _ClassVar[int]
    REFRESH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    api_id: bytes
    access_token: str
    refresh_token: str
    def __init__(self, api_id: _Optional[bytes] = ..., access_token: _Optional[str] = ..., refresh_token: _Optional[str] = ...) -> None: ...

class UserRefreshResponse(_message.Message):
    __slots__ = ("access_token", "refresh_token")
    ACCESS_TOKEN_FIELD_NUMBER: _ClassVar[int]
    REFRESH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    access_token: str
    refresh_token: str
    def __init__(self, access_token: _Optional[str] = ..., refresh_token: _Optional[str] = ...) -> None: ...

class UserLogoutRequest(_message.Message):
    __slots__ = ("user_id", "auth_token")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    AUTH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    user_id: bytes
    auth_token: str
    def __init__(self, user_id: _Optional[bytes] = ..., auth_token: _Optional[str] = ...) -> None: ...

class UserLogoutResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
