import re
from datetime import datetime
from typing import Annotated, Any, Literal, Union
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, RootModel, field_validator, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra='forbid', populate_by_name=False)


class Join(StrictModel):
    request_id: UUID
    poll_secret: str = Field(pattern=r'^[A-Za-z0-9_-]{43}$')
    agent_name: str = Field(pattern=r'^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$')
    display_name: str = Field(min_length=1, max_length=200)
    owner_name: str = Field(min_length=1, max_length=200)
    owner_contact: str = Field(min_length=1, max_length=320)
    platform: str = Field(min_length=1, max_length=100)
    purpose: str = Field(min_length=1, max_length=2000)
    room_code: str | None = Field(default=None, max_length=256)

    @field_validator('request_id')
    @classmethod
    def v4(cls, value):
        if value.version != 4:
            raise ValueError('request_id must be UUIDv4')
        return value


class Chat(StrictModel):
    text: str = Field(max_length=8000)


class RequestBody(StrictModel):
    action: str = Field(max_length=64)
    params: dict[str, Any]


class ResponseBody(StrictModel):
    ok: bool = Field(strict=True)
    result: dict[str, Any] | None = None
    error: str | None = None


class Event(StrictModel):
    name: str = Field(max_length=64)
    data: dict[str, Any]


class Terms(StrictModel):
    terms: dict[str, Any]
    expires_at: AwareDatetime | None = None

    @field_validator('terms')
    @classmethod
    def no_floats(cls, value):
        def walk(obj):
            if isinstance(obj, float):
                raise ValueError('Floats are forbidden in terms; use strings or integers')
            if isinstance(obj, dict):
                for v in obj.values():
                    walk(v)
            if isinstance(obj, list):
                for v in obj:
                    walk(v)
        walk(value)
        return value


class Propose(Terms):
    subject: str = Field(min_length=1, max_length=200)


class Counter(Terms):
    negotiation_id: UUID
    base_version: int = Field(ge=1, strict=True)
    note: str | None = Field(default=None, max_length=1000)


class Accept(StrictModel):
    negotiation_id: UUID
    version: int = Field(ge=1, strict=True)
    terms_hash: str = Field(pattern=r'^[a-f0-9]{64}$')


class Decline(StrictModel):
    negotiation_id: UUID
    version: int = Field(ge=1, strict=True)
    reason: str | None = Field(default=None, max_length=1000)


class Withdraw(StrictModel):
    negotiation_id: UUID
    reason: str | None = Field(default=None, max_length=1000)


class Envelope(StrictModel):
    id: UUID
    nonce: str = Field(pattern=r'^[A-Za-z0-9_-]{22,64}$')
    ts: AwareDatetime
    sender: str | None = Field(default=None, alias='from')
    to: list[str] | None = Field(default=None, min_length=1, max_length=8, json_schema_extra={'uniqueItems': True})
    thread_id: UUID
    reply_to: UUID | None = None

    @model_validator(mode='before')
    @classmethod
    def explicit_fields(cls, data):
        if isinstance(data, dict) and ('to' in data and data['to'] is None or 'from' in data and data['from'] is None):
            raise ValueError('Omit to/from instead of passing null')
        if isinstance(data, dict) and (not isinstance(data.get('ts'), str) or not re.search(r'(Z|[+-]\d{2}:\d{2})$', data['ts'])):
            raise ValueError('ts must be RFC 3339 with an explicit offset')
        return data

    @field_validator('id')
    @classmethod
    def valid_id(cls, value):
        if value.version not in (4, 7):
            raise ValueError('id must be UUIDv4 or UUIDv7')
        return value

    @field_validator('to')
    @classmethod
    def unique(cls, value):
        if value is not None and len(value) != len(set(value)):
            raise ValueError('Recipients must be unique')
        return value


class ChatMessage(Envelope):
    type: Literal['chat']
    body: Chat


class RequestMessage(Envelope):
    type: Literal['request']
    body: RequestBody


class ResponseMessage(Envelope):
    type: Literal['response']
    body: ResponseBody

    @model_validator(mode='after')
    def has_reply(self):
        if self.reply_to is None:
            raise ValueError('response requires reply_to')
        return self


class EventMessage(Envelope):
    type: Literal['event']
    body: Event


class ProposeMessage(Envelope):
    type: Literal['propose']
    body: Propose


class CounterMessage(Envelope):
    type: Literal['counter']
    body: Counter


class AcceptMessage(Envelope):
    type: Literal['accept']
    body: Accept


class DeclineMessage(Envelope):
    type: Literal['decline']
    body: Decline


class WithdrawMessage(Envelope):
    type: Literal['withdraw']
    body: Withdraw


Message = Annotated[Union[ChatMessage, RequestMessage, ResponseMessage, EventMessage, ProposeMessage,
                          CounterMessage, AcceptMessage, DeclineMessage, WithdrawMessage], Field(discriminator='type')]


class AgentRoomMessageSubmit(RootModel[Message]):
    pass


class Refresh(StrictModel):
    refresh_token: str = Field(pattern=r'^art_[A-Za-z0-9_-]{43}$')
    request_id: UUID | None = None


class Reconnect(StrictModel):
    ticket_id: UUID
    secret: str = Field(pattern=r'^[A-Za-z0-9_-]{43}$')


class Ack(StrictModel):
    seq: int = Field(ge=0, strict=True)


class Approve(StrictModel):
    role: str = Field(default='member', max_length=64)


class Deny(StrictModel):
    reason: str = Field(default='', max_length=1000)
