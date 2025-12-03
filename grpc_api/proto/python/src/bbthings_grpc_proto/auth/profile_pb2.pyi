from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RoleProfileSchema(_message.Message):
    __slots__ = ("id", "role_id", "name", "value_type", "mode")
    ID_FIELD_NUMBER: _ClassVar[int]
    ROLE_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    id: int
    role_id: bytes
    name: str
    value_type: int
    mode: int
    def __init__(self, id: _Optional[int] = ..., role_id: _Optional[bytes] = ..., name: _Optional[str] = ..., value_type: _Optional[int] = ..., mode: _Optional[int] = ...) -> None: ...

class UserProfileSchema(_message.Message):
    __slots__ = ("id", "user_id", "name", "value_bytes", "value_type", "order")
    ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_BYTES_FIELD_NUMBER: _ClassVar[int]
    VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
    ORDER_FIELD_NUMBER: _ClassVar[int]
    id: int
    user_id: bytes
    name: str
    value_bytes: bytes
    value_type: int
    order: int
    def __init__(self, id: _Optional[int] = ..., user_id: _Optional[bytes] = ..., name: _Optional[str] = ..., value_bytes: _Optional[bytes] = ..., value_type: _Optional[int] = ..., order: _Optional[int] = ...) -> None: ...

class ProfileId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class RoleId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class UserId(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: bytes
    def __init__(self, id: _Optional[bytes] = ...) -> None: ...

class RoleProfileUpdate(_message.Message):
    __slots__ = ("id", "name", "value_type", "mode")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    value_type: int
    mode: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., value_type: _Optional[int] = ..., mode: _Optional[int] = ...) -> None: ...

class UserProfileUpdate(_message.Message):
    __slots__ = ("id", "name", "value_bytes", "value_type")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    VALUE_BYTES_FIELD_NUMBER: _ClassVar[int]
    VALUE_TYPE_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    value_bytes: bytes
    value_type: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., value_bytes: _Optional[bytes] = ..., value_type: _Optional[int] = ...) -> None: ...

class UserProfileSwap(_message.Message):
    __slots__ = ("user_id", "name", "order_1", "order_2")
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    ORDER_1_FIELD_NUMBER: _ClassVar[int]
    ORDER_2_FIELD_NUMBER: _ClassVar[int]
    user_id: bytes
    name: str
    order_1: int
    order_2: int
    def __init__(self, user_id: _Optional[bytes] = ..., name: _Optional[str] = ..., order_1: _Optional[int] = ..., order_2: _Optional[int] = ...) -> None: ...

class RoleProfileReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: RoleProfileSchema
    def __init__(self, result: _Optional[_Union[RoleProfileSchema, _Mapping]] = ...) -> None: ...

class RoleProfileListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[RoleProfileSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[RoleProfileSchema, _Mapping]]] = ...) -> None: ...

class UserProfileReadResponse(_message.Message):
    __slots__ = ("result",)
    RESULT_FIELD_NUMBER: _ClassVar[int]
    result: UserProfileSchema
    def __init__(self, result: _Optional[_Union[UserProfileSchema, _Mapping]] = ...) -> None: ...

class UserProfileListResponse(_message.Message):
    __slots__ = ("results",)
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    results: _containers.RepeatedCompositeFieldContainer[UserProfileSchema]
    def __init__(self, results: _Optional[_Iterable[_Union[UserProfileSchema, _Mapping]]] = ...) -> None: ...

class ProfileCreateResponse(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: int
    def __init__(self, id: _Optional[int] = ...) -> None: ...

class ProfileChangeResponse(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...
