from typing import List, Union
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from ..common.type_value import DataType, unpack_data


@dataclass
class ProcedureSchema:
    id: UUID
    api_id: UUID
    name: str
    description: str
    roles: List[str]

    def from_response(r):
        roles = []
        for p in r.roles: roles.append(str(p))
        return ProcedureSchema(UUID(bytes=r.id), UUID(bytes=r.api_id), r.name, r.description, roles)


@dataclass
class ApiSchema:
    id: UUID
    name: str
    address: str
    category: str
    description: str
    password: str
    access_key: bytes
    procedures: List[ProcedureSchema]

    def from_response(r):
        procedures = []
        for p in r.procedures: procedures.append(ProcedureSchema(UUID(bytes=p.id), UUID(bytes=p.api_id), p.name, p.description, p.roles))
        return ApiSchema(UUID(bytes=r.id), r.name, r.address, r.category, r.description, r.password, r.access_key, procedures)


@dataclass
class RoleSchema:
    id: UUID
    api_id: UUID
    name: str
    multi: bool
    ip_lock: bool
    access_duration: int
    refresh_duration: int
    access_key: bytes
    procedures: List[UUID]

    def from_response(r):
        procedures = []
        for p in r.procedures: procedures.append(UUID(bytes=p))
        return RoleSchema(UUID(bytes=r.id), UUID(bytes=r.api_id), r.name, r.multi, r.ip_lock, r.access_duration, r.refresh_duration, r.access_key, procedures)


@dataclass
class UserRoleSchema:
    api_id: UUID
    role: str
    multi: bool
    ip_lock: bool
    access_duration: int
    refresh_duration: int
    access_key: bytes

    def from_response(r):
        return UserRoleSchema(UUID(bytes=r.api_id), r.role, r.multi, r.ip_lock, r.access_duration, r.refresh_duration, r.access_key)


@dataclass
class UserSchema:
    id: UUID
    name: str
    email: str
    phone: str
    password: str
    roles: List[UserRoleSchema]

    def from_response(r):
        user_roles = []
        for p in r.roles: user_roles.append(UserRoleSchema.from_response(p))
        return UserSchema(UUID(bytes=r.id), r.name, r.email, r.phone, r.password, user_roles)


class ProfileMode:
    SINGLE_OPTIONAL = 0
    SINGLE_REQUIRED = 1
    MULTIPLE_OPTIONAL = 2
    MULTIPLE_REQUIRED = 3

    @staticmethod
    def from_str(mode: str) -> int:
        if mode == "SINGLE_REQUIRED":
            return 1
        elif mode == "MULTIPLE_OPTIONAL":
            return 2
        elif mode == "MULTIPLE_REQUIRED":
            return 3
        else:
            return 0

    @staticmethod
    def to_str(mode: int) -> str:
        if mode == 1:
            return "SINGLE_REQUIRED"
        elif mode == 2:
            return "MULTIPLE_OPTIONAL"
        elif mode == 3:
            return "MULTIPLE_REQUIRED"
        else:
            return "SINGLE_OPTIONAL"

    @staticmethod
    def from_int_str(mode: Union[int, str]) -> int:
        if isinstance(mode, int):
            if mode >= 1 and mode <= 3: return mode
            else: return 0
        elif isinstance(mode, str):
            return ProfileMode.from_str(mode)
        else:
            return 0


@dataclass
class RoleProfileSchema:
    id: int
    role_id: UUID
    name: str
    value_type: DataType
    mode: int

    def from_response(r):
        mode = int(r.mode)
        return RoleProfileSchema(r.id, UUID(bytes=r.role_id), r.name, DataType(r.value_type), mode)


@dataclass
class UserProfileSchema:
    id: int
    user_id: UUID
    name: str
    value: List[Union[bool, int, float, str, None]]
    order: int

    def from_response(r):
        value = unpack_data(r.value_bytes, DataType(r.value_type))
        return UserProfileSchema(r.id, UUID(bytes=r.user_id), r.name, value, r.order)


@dataclass
class TokenSchema:
    access_id: int
    user_id: UUID
    refresh_token: str
    auth_token: str
    expire: datetime
    ip: bytes

    def from_response(r):
        return TokenSchema(r.access_id, UUID(bytes=r.user_id), r.refresh_token, r.auth_token, datetime.fromtimestamp(r.expire/1000000.0), r.ip)


@dataclass
class AccessToken:
    api_id: UUID
    access_token: str
    refresh_token: str


@dataclass
class UserLogin:
    user_id: UUID
    auth_token: str
    access_tokens: List[AccessToken]


@dataclass
class UserRefresh:
    access_token: str
    refresh_token: str
