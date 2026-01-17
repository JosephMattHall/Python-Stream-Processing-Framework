from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class ItemCreated(_message.Message):
    __slots__ = ("item_id", "name", "owner")
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    item_id: str
    name: str
    owner: str
    def __init__(self, item_id: _Optional[str] = ..., name: _Optional[str] = ..., owner: _Optional[str] = ...) -> None: ...

class ItemCheckedIn(_message.Message):
    __slots__ = ("item_id", "qty", "user_id")
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    QTY_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    item_id: str
    qty: int
    user_id: str
    def __init__(self, item_id: _Optional[str] = ..., qty: _Optional[int] = ..., user_id: _Optional[str] = ...) -> None: ...

class ItemCheckedOut(_message.Message):
    __slots__ = ("item_id", "qty", "user_id")
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    QTY_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    item_id: str
    qty: int
    user_id: str
    def __init__(self, item_id: _Optional[str] = ..., qty: _Optional[int] = ..., user_id: _Optional[str] = ...) -> None: ...

class CheckoutRejected(_message.Message):
    __slots__ = ("item_id", "requested_qty", "available_qty", "reason")
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    REQUESTED_QTY_FIELD_NUMBER: _ClassVar[int]
    AVAILABLE_QTY_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    item_id: str
    requested_qty: int
    available_qty: int
    reason: str
    def __init__(self, item_id: _Optional[str] = ..., requested_qty: _Optional[int] = ..., available_qty: _Optional[int] = ..., reason: _Optional[str] = ...) -> None: ...
