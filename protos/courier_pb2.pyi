from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class JobCreated(_message.Message):
    __slots__ = ("job_id", "customer_id", "pickup_address", "delivery_address")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    PICKUP_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    DELIVERY_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    customer_id: str
    pickup_address: str
    delivery_address: str
    def __init__(self, job_id: _Optional[str] = ..., customer_id: _Optional[str] = ..., pickup_address: _Optional[str] = ..., delivery_address: _Optional[str] = ...) -> None: ...

class DriverOnline(_message.Message):
    __slots__ = ("driver_id", "vehicle_type")
    DRIVER_ID_FIELD_NUMBER: _ClassVar[int]
    VEHICLE_TYPE_FIELD_NUMBER: _ClassVar[int]
    driver_id: str
    vehicle_type: str
    def __init__(self, driver_id: _Optional[str] = ..., vehicle_type: _Optional[str] = ...) -> None: ...

class JobAssigned(_message.Message):
    __slots__ = ("job_id", "driver_id", "timestamp")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    DRIVER_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    driver_id: str
    timestamp: str
    def __init__(self, job_id: _Optional[str] = ..., driver_id: _Optional[str] = ..., timestamp: _Optional[str] = ...) -> None: ...

class JobDelivered(_message.Message):
    __slots__ = ("job_id", "timestamp")
    JOB_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    job_id: str
    timestamp: str
    def __init__(self, job_id: _Optional[str] = ..., timestamp: _Optional[str] = ...) -> None: ...
