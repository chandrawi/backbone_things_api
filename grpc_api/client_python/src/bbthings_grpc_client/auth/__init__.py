from datetime import datetime
from uuid import UUID
from typing import Optional, Union, List, Tuple
from . import auth, api, role, user, profile, token
from ..common.type_value import DataType
from ._schema import ApiSchema, ProcedureSchema, RoleSchema, UserSchema, \
    RoleProfileSchema, UserProfileSchema, TokenSchema, UserLogin, UserRefresh


class Auth:

    def __init__(self, address: str, auth_token: Optional[str] = None):
        self.address = address
        self.metadata = [] if auth_token == None \
            else [("authorization", "Bearer " + auth_token)]

    def user_login(self, username: str, password: str) -> UserLogin:
        return auth.user_login(self, username, password)

    def user_refresh(self, api_id: UUID, access_token: str, refresh_token: str) -> UserRefresh:
        return auth.user_refresh(self, api_id, access_token, refresh_token)

    def user_logout(self, user_id: UUID, auth_token: str):
        return auth.user_logout(self, user_id, auth_token)

    def read_api(self, id: UUID) -> ApiSchema:
        return api.read_api(self, id)

    def read_api_by_name(self, name: str) -> ApiSchema:
        return api.read_api_by_name(self, name)

    def list_api_by_ids(self, ids: List[UUID]) -> List[ApiSchema]:
        api.list_api_by_ids(self, ids)

    def list_api_by_name(self, name: str) -> List[ApiSchema]:
        api.list_api_by_name(self, name)

    def list_api_by_category(self, category: str) -> List[ApiSchema]:
        return api.list_api_by_category(self, category)

    def list_api_option(self, name: Optional[str], category: Optional[str]) -> List[ApiSchema]:
        api.list_api_option(self, name, category)

    def create_api(self, id: UUID, name: str, address: str, category: str, description: str, password: str, access_key: bytes) -> UUID:
        return api.create_api(self, id, name, address, category, description, password, access_key)

    def update_api(self, id: UUID, name: Optional[str]=None, address: Optional[str]=None, category: Optional[str]=None, description: Optional[str]=None, password: Optional[str]=None, access_key: Optional[bytes]=None):
        return api.update_api(self, id, name, address, category, description, password, access_key)

    def delete_api(self, id: UUID):
        return api.delete_api(self, id)

    def read_procedure(self, id: UUID) -> ProcedureSchema:
        return api.read_procedure(self, id)

    def read_procedure_by_name(self, api_id: UUID, name: str) -> ProcedureSchema:
        return api.read_procedure_by_name(self, api_id, name)

    def list_procedure_by_ids(self, ids: List[UUID]) -> List[ProcedureSchema]:
        api.list_procedure_by_ids(self, ids)

    def list_procedure_by_api(self, api_id: UUID) -> List[ProcedureSchema]:
        return api.list_procedure_by_api(self, api_id)

    def list_procedure_by_name(self, name: str) -> List[ProcedureSchema]:
        api.list_procedure_by_name(self, name)

    def list_procedure_option(self, api_id: Optional[UUID], name: Optional[str]) -> List[ProcedureSchema]:
        api.list_procedure_option(self, None, api_id, None, name)

    def create_procedure(self, id: UUID, api_id: UUID, name: str, description: str) -> UUID:
        return api.create_procedure(self, id, api_id, name, description)

    def update_procedure(self, id: UUID, name: Optional[str], description: Optional[str]):
        return api.update_procedure(self, id, name, description)

    def delete_procedure(self, id: UUID):
        return api.delete_procedure(self, id)

    def read_role(self, id: UUID) -> RoleSchema:
        return role.read_role(self, id)

    def read_role_by_name(self, api_id: UUID, name: str) -> RoleSchema:
        return role.read_role_by_name(self, api_id, name)

    def list_role_by_api(self, api_id: UUID) -> List[RoleSchema]:
        return role.list_role_by_api(self, api_id)
    
    def list_role_by_user(self, user_id: UUID) -> List[RoleSchema]:
        return role.list_role_by_user(self, user_id)

    def list_role_by_ids(self, ids: List[UUID]) -> List[RoleSchema]:
        role.list_role_by_ids(self, ids)

    def list_role_by_name(self, name: str) -> List[RoleSchema]:
        role.list_role_by_name(self, name)

    def list_role_option(self, api_id: Optional[UUID], user_id: Optional[UUID], name: Optional[str]) -> List[RoleSchema]:
        role.list_role_option(self, api_id, user_id, name)

    def create_role(self, id: UUID, api_id: UUID, name: str, multi: bool, ip_lock: bool, access_duration: int, refresh_duration: int) -> UUID:
        return role.create_role(self, id, api_id, name, multi, ip_lock, access_duration, refresh_duration)

    def update_role(self, id: UUID, name: Optional[str]=None, multi: Optional[bool]=None, ip_lock: Optional[bool]=None, access_duration: Optional[int]=None, refresh_duration: Optional[int]=None):
        return role.update_role(self, id, name, multi, ip_lock, access_duration, refresh_duration)

    def delete_role(self, id: UUID):
        return role.delete_role(self, id)

    def add_role_access(self, id: UUID, procedure_id: UUID):
        return role.add_role_access(self, id, procedure_id)

    def remove_role_access(self, id: UUID, procedure_id: UUID):
        return role.remove_role_access(self, id, procedure_id)

    def read_role_profile(self, id: int) -> RoleProfileSchema:
        return profile.read_role_profile(self, id)

    def list_role_profile_by_role(self, role_id: UUID) -> List[RoleProfileSchema]:
        return profile.list_role_profile_by_role(self, role_id)

    def create_role_profile(self, role_id: UUID, name: str, value_type: DataType, mode: Union[str, int]) -> int:
        return profile.create_role_profile(self, role_id, name, value_type, mode)

    def update_role_profile(self, id: int, name: Optional[str], value_type: Optional[DataType], mode: Optional[Union[str, int]]):
        return profile.update_role_profile(self, id, name, value_type, mode)

    def delete_role_profile(self, id: int):
        return profile.delete_role_profile(self, id)

    def read_user(self, id: UUID) -> UserSchema:
        return user.read_user(self, id)

    def read_user_by_name(self, name: str) -> UserSchema:
        return user.read_user_by_name(self, name)

    def list_user_by_ids(self, ids: List[UUID]) -> List[UserSchema]:
        user.list_user_by_ids(self, ids)

    def list_user_by_api(self, api_id: UUID) -> List[UserSchema]:
        user.list_user_by_api(self, api_id)

    def list_user_by_role(self, role_id: UUID) -> List[UserSchema]:
        return user.list_user_by_role(self, role_id)

    def list_user_by_name(self, name: str) -> List[UserSchema]:
        user.list_user_by_name(self, name)

    def list_user_option(self, api_id: Optional[UUID], role_id: Optional[UUID], name: Optional[str]) -> List[UserSchema]:
        user.list_user_by_name(self, None, api_id, role_id, None, name)

    def create_user(self, id: UUID, name: str, email: str, phone: str, password: str) -> UUID:
        return user.create_user(self, id, name, email, phone, password)

    def update_user(self, id: UUID, name: Optional[str]=None, email: Optional[str]=None, phone: Optional[str]=None, password: Optional[str]=None):
        return user.update_user(self, id, name, email, phone, password)

    def delete_user(self, id: UUID):
        return user.delete_user(self, id)

    def add_user_role(self, id: UUID, role_id: UUID):
        return user.add_user_role(self, id, role_id)

    def remove_user_role(self, id: UUID, role_id: UUID):
        return user.remove_user_role(self, id, role_id)

    def read_user_profile(self, id: int) -> UserProfileSchema:
        return profile.read_user_profile(self, id)

    def list_user_profile_by_user(self, user_id: UUID) -> List[UserProfileSchema]:
        return profile.list_user_profile_by_user(self, user_id)

    def create_user_profile(self, user_id: UUID, name: str, value: Union[int, float, str, bool, None]) -> int:
        return profile.create_user_profile(self, user_id, name, value)

    def update_user_profile(self, id: int, name: Optional[str], value: Optional[Union[int, float, str, bool, None]]):
        return profile.update_user_profile(self, id, name, value)

    def delete_user_profile(self, id: int):
        return profile.delete_user_profile(self, id)

    def swap_user_profile(self, user_id: UUID, name: str, order_1: int, order_2: int):
        return profile.swap_user_profile(self, user_id, name, order_1, order_2)

    def read_access_token(self, access_id: int) -> TokenSchema:
        return token.read_access_token(self, access_id)

    def list_auth_token(self, auth_token: str) -> List[TokenSchema]:
        return token.list_auth_token(self, auth_token)
    
    def list_token_by_user(self, user_id: UUID) -> List[TokenSchema]:
        return token.list_token_by_user(self, user_id)

    def list_token_by_created_earlier(self, earlier: datetime, user_id: Optional[UUID]=None) -> List[TokenSchema]:
        return token.list_token_by_created_earlier(self, earlier, user_id)

    def list_token_by_created_later(self, later: datetime, user_id: Optional[UUID]=None) -> List[TokenSchema]:
        return token.list_token_by_created_later(self, later, user_id)

    def list_token_by_created_range(self, begin: datetime, end: datetime, user_id: Optional[UUID]=None) -> List[TokenSchema]:
        return token.list_token_by_created_range(self, begin, end, user_id)

    def list_token_by_expired_earlier(self, earlier: datetime, user_id: Optional[UUID]=None) -> List[TokenSchema]:
        return token.list_token_by_expired_earlier(self, earlier, user_id)

    def list_token_by_expired_later(self, later: datetime, user_id: Optional[UUID]=None) -> List[TokenSchema]:
        return token.list_token_by_expired_later(self, later, user_id)

    def list_token_by_expired_range(self, begin: datetime, end: datetime, user_id: Optional[UUID]=None) -> List[TokenSchema]:
        return token.list_token_by_expired_range(self, begin, end, user_id)

    def list_token_by_range(self, b_created: datetime, e_created: datetime, b_expired: datetime, e_expired: datetime, user_id: Optional[UUID]=None) -> List[TokenSchema]:
        return token.list_token_by_range(self, b_created, e_created, b_expired, e_expired, user_id)

    def create_access_token(self, user_id: UUID, auth_token: str, expire: datetime, ip: bytes) -> Tuple[int, str, str]:
        return token.create_access_token(self, user_id, auth_token, expire, ip)

    def create_auth_token(self, user_id: UUID, expire: datetime, ip: bytes, number: int) -> List[Tuple[int, str, str]]:
        return token.create_auth_token(self, user_id, expire, ip, number)

    def update_access_token(self, access_id: int, expire: Optional[datetime]=None, ip: Optional[bytes]=None) -> Tuple[str, str]:
        return token.update_access_token(self, access_id, expire, ip)

    def update_auth_token(self, auth_token: str, expire: Optional[datetime]=None, ip: Optional[bytes]=None) -> Tuple[str, str]:
        return token.update_auth_token(self, auth_token, expire, ip)

    def delete_access_token(self, access_id: int):
        return token.delete_access_token(self, access_id)

    def delete_auth_token(self, auth_token: str):
        return token.delete_auth_token(self, auth_token)
    
    def delete_token_by_user(self, user_id: UUID):
        return token.delete_token_by_user(self, user_id)
