from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TokenSchema(_message.Message):
    __slots__ = ("access_id", "user_id", "refresh_token", "auth_token", "created", "expired", "ip")
    ACCESS_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    REFRESH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    AUTH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    EXPIRED_FIELD_NUMBER: _ClassVar[int]
    IP_FIELD_NUMBER: _ClassVar[int]
    access_id: int
    user_id: bytes
    refresh_token: str
    auth_token: str
    created: int
    expired: int
    ip: bytes
    def __init__(self, access_id: _Optional[int] = ..., user_id: _Optional[bytes] = ..., refresh_token: _Optional[str] = ..., auth_token: _Optional[str] = ..., created: _Optional[int] = ..., expired: _Optional[int] = ..., ip: _Optional[bytes] = ...) -> None: ...

class AuthToken(_message.Message):
    __slots__ = ("auth_token",)
    AUTH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    auth_token: str
    def __init__(self, auth_token: _Optional[str] = ...) -> None: ...

class AccessId(_message.Message):
    __slots__ = ("access_id",)
    ACCESS_ID_FIELD_NUMBER: _ClassVar[int]
    access_id: int
    def __init__(self, access_id: _Optional[int] = ...) -> None: ...

class UserId(_message.Message):
    __slots__ = ("user_id",)
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    user_id: bytes
    def __init__(self, user_id: _Optional[bytes] = ...) -> None: ...

class TokenTime(_message.Message):
    __slots__ = ("timestamp", "user_id")
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    timestamp: int
    user_id: bytes
    def __init__(self, timestamp: _Optional[int] = ..., user_id: _Optional[bytes] = ...) -> None: ...

class TokenRangeSingle(_message.Message):
    __slots__ = ("begin", "end", "user_id")
    BEGIN_FIELD_NUMBER: _ClassVar[int]
    END_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    begin: int
    end: int
    user_id: bytes
    def __init__(self, begin: _Optional[int] = ..., end: _Optional[int] = ..., user_id: _Optional[bytes] = ...) -> None: ...

class TokenRangeDouble(_message.Message):
    __slots__ = ("begin_1", "end_1", "begin_2", "end_2", "user_id")
    BEGIN_1_FIELD_NUMBER: _ClassVar[int]
    END_1_FIELD_NUMBER: _ClassVar[int]
    BEGIN_2_FIELD_NUMBER: _ClassVar[int]
    END_2_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    begin_1: int
    end_1: int
    begin_2: int
    end_2: int
    user_id: bytes
    def __init__(self, begin_1: _Optional[int] = ..., end_1: _Optional[int] = ..., begin_2: _Optional[int] = ..., end_2: _Optional[int] = ..., user_id: _Optional[bytes] = ...) -> None: ...

class AuthTokenCreate(_message.Message):
    __slots__ = ("user_id", "expired", "ip", "number")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    EXPIRED_FIELD_NUMBER: _ClassVar[int]
    IP_FIELD_NUMBER: _ClassVar[int]
    NUMBER_FIELD_NUMBER: _ClassVar[int]
    user_id: bytes
    expired: int
    ip: bytes
    number: int
    def __init__(self, user_id: _Optional[bytes] = ..., expired: _Optional[int] = ..., ip: _Optional[bytes] = ..., number: _Optional[int] = ...) -> None: ...

class TokenUpdate(_message.Message):
    __slots__ = ("access_id", "refresh_token", "auth_token", "expired", "ip")
    ACCESS_ID_FIELD_NUMBER: _ClassVar[int]
    REFRESH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    AUTH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    EXPIRED_FIELD_NUMBER: _ClassVar[int]
    IP_FIELD_NUMBER: _ClassVar[int]
    access_id: int
    refresh_token: str
    auth_token: str
    expired: int
    ip: bytes
    def __init__(self, access_id: _Optional[int] = ..., refresh_token: _Optional[str] = ..., auth_token: _Optional[str] = ..., expired: _Optional[int] = ..., ip: _Optional[bytes] = ...) -> None: ...

class TokenReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: TokenSchema
    def __init__(self, result: _Optional[_Union[TokenSchema, _Mapping]] = ...) -> None: ...

class TokenListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[TokenSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[TokenSchema, _Mapping]]] = ...) -> None: ...

class TokenCreateResponse(_message.Message):
    __slots__ = ("access_id", "refresh_token", "auth_token")
    ACCESS_ID_FIELD_NUMBER: _ClassVar[int]
    REFRESH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    AUTH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    access_id: int
    refresh_token: str
    auth_token: str
    def __init__(self, access_id: _Optional[int] = ..., refresh_token: _Optional[str] = ..., auth_token: _Optional[str] = ...) -> None: ...

class AuthTokenCreateResponse(_message.Message):
    __slots__ = ("tokens",)
    TOKENS_FIELD_NUMBER: _ClassVar[int]
    tokens: _containers.RepeatedCompositeFieldContainer[TokenCreateResponse]
    def __init__(self, tokens: _Optional[_Iterable[_Union[TokenCreateResponse, _Mapping]]] = ...) -> None: ...

class TokenUpdateResponse(_message.Message):
    __slots__ = ("refresh_token",)
    REFRESH_TOKEN_FIELD_NUMBER: _ClassVar[int]
    refresh_token: str
    def __init__(self, refresh_token: _Optional[str] = ...) -> None: ...

class TokenChangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
