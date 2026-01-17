from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Transaction(_message.Message):
    __slots__ = ("transaction_id", "user_id", "amount", "merchant", "timestamp")
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_FIELD_NUMBER: _ClassVar[int]
    MERCHANT_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    transaction_id: str
    user_id: str
    amount: float
    merchant: str
    timestamp: str
    def __init__(self, transaction_id: _Optional[str] = ..., user_id: _Optional[str] = ..., amount: _Optional[float] = ..., merchant: _Optional[str] = ..., timestamp: _Optional[str] = ...) -> None: ...

class FraudAlert(_message.Message):
    __slots__ = ("alert_id", "user_id", "reason", "transaction_amount", "avg_spend", "timestamp")
    ALERT_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    TRANSACTION_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    AVG_SPEND_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    alert_id: str
    user_id: str
    reason: str
    transaction_amount: float
    avg_spend: float
    timestamp: str
    def __init__(self, alert_id: _Optional[str] = ..., user_id: _Optional[str] = ..., reason: _Optional[str] = ..., transaction_amount: _Optional[float] = ..., avg_spend: _Optional[float] = ..., timestamp: _Optional[str] = ...) -> None: ...
