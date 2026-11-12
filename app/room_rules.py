import json
import re

from pydantic import ConfigDict, Field, field_validator

from . import db
from .errors import APIError
from .models import StrictModel

MEMBER_TYPES = ('chat', 'request', 'response', 'event', 'propose', 'counter', 'accept', 'decline', 'withdraw')
TOKEN = re.compile(r'^[A-Za-z0-9_.:@/-]{1,80}$')
SCHEMA = 'agentroom.rules/1'


class EnforcedRules(StrictModel):
    message_types: list[str] | None = None
    identifier_values: bool = False
    require_acknowledgement: bool = False
    max_text_length: int | None = Field(default=None, ge=1, le=8000)

    @field_validator('message_types')
    @classmethod
    def known_types(cls, value):
        if value is not None and (not value or len(value) != len(set(value)) or any(item not in MEMBER_TYPES for item in value)):
            raise ValueError('message_types must be a unique non-empty subset of member message types')
        return value


class AckVersion(StrictModel):
    version: int = Field(ge=1, strict=True)


class RulesDocument(StrictModel):
    model_config = ConfigDict(extra='forbid', populate_by_name=True)
    schema_name: str = Field(alias='schema', pattern='^agentroom\\.rules/1$')
    instructions: str = Field(min_length=1, max_length=8000)
    enforced: EnforcedRules = Field(default_factory=EnforcedRules)


def canonical(document):
    return json.dumps(document, sort_keys=True, separators=(',', ':'), ensure_ascii=True)


def parse(payload):
    try:
        model = RulesDocument.model_validate(payload)
    except Exception:
        raise APIError(422, 'validation_error', 'Invalid room rules document') from None
    return json.loads(model.model_dump_json(by_alias=True))


def active(conn):
    return db.one(conn, 'SELECT version, document, created_at FROM room_rules ORDER BY version DESC LIMIT 1')


def acknowledged(conn, member_id, version):
    return db.one(conn, 'SELECT 1 AS ok FROM room_rule_acks WHERE member_id=? AND version=?', (member_id, version)) is not None


def view(conn, member_id=None):
    row = active(conn)
    if not row:
        return {'schema': SCHEMA, 'version': None, 'instructions': None, 'enforced': None, 'acknowledged': False,
                'note': 'No room rules are published. Existing message behavior is unchanged. Instructions are behavioral; enforced controls are checked on send.'}
    document = json.loads(row['document'])
    version = row['version']
    return {'schema': SCHEMA, 'version': version, 'instructions': document['instructions'], 'enforced': document['enforced'],
            'acknowledged': bool(member_id and acknowledged(conn, member_id, version)),
            'published_at': row['created_at'],
            'note': 'Instructions tell an agent what to do. Enforced controls are the only rules the server can guarantee. Amounts and documents belong in the product, not in room messages.'}


def publish(conn, payload):
    document = parse(payload)
    encoded = canonical(document)
    current = active(conn)
    if current and current['document'] == encoded:
        raise APIError(409, 'rules_unchanged', 'The active room rules already match this document')
    version = (current['version'] if current else 0) + 1
    conn.execute('INSERT INTO room_rules(version, document, created_at) VALUES(?,?,?)', (version, encoded, db.stamp()))
    return version


def acknowledge(conn, member_id, version):
    row = active(conn)
    if not row:
        raise APIError(409, 'rules_absent', 'This room has no rules to acknowledge')
    if version != row['version']:
        raise APIError(409, 'rules_version', 'Acknowledge the active room rules version')
    if not acknowledged(conn, member_id, version):
        conn.execute('INSERT INTO room_rule_acks(member_id, version, acked_at) VALUES(?,?,?)', (member_id, version, db.stamp()))
    return version


def _identifiers(value):
    if isinstance(value, bool) or value is None:
        return
    if isinstance(value, int):
        return
    if isinstance(value, str):
        if not TOKEN.fullmatch(value):
            raise ValueError('prose')
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not TOKEN.fullmatch(str(key)):
                raise ValueError('prose')
            _identifiers(item)
        return
    if isinstance(value, list):
        for item in value:
            _identifiers(item)
        return
    raise ValueError('prose')


def enforce_operation(conn, actor, message_type, body):
    text = body.get('text', '') if isinstance(body, dict) else ''

    class PolicyBody:
        def model_dump(self, mode='json'):
            return body

    model = type('PolicyMessage', (), {})()
    model.type = message_type
    model.body = PolicyBody()
    model.body.text = text
    enforce(conn, actor, model)


def enforce(conn, actor, model):
    row = active(conn)
    if not row:
        return
    document = json.loads(row['document'])
    enforced = document.get('enforced') or {}
    version = row['version']
    if enforced.get('require_acknowledgement') and not acknowledged(conn, actor['member_id'], version):
        raise APIError(403, 'rules_unacknowledged', f'Acknowledge room rules version {version} before sending')
    allowed = enforced.get('message_types')
    if allowed is not None and model.type not in allowed:
        raise APIError(403, 'rule_violation', 'This message type is not allowed by the active room rules')
    limit = enforced.get('max_text_length')
    if limit and model.type == 'chat' and len(model.body.text) > limit:
        raise APIError(403, 'rule_violation', 'Chat text exceeds the active room rule')
    if enforced.get('identifier_values'):
        try:
            _identifiers(model.body.model_dump(mode='json'))
        except ValueError:
            raise APIError(403, 'rule_violation', 'The active room rules allow identifier values only') from None
