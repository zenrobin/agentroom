import copy
import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from app.models import AgentRoomMessageSubmit
from .conftest import message


def generated_schema():
    return {'$schema': 'https://json-schema.org/draft/2020-12/schema', **AgentRoomMessageSubmit.model_json_schema()}


def test_schema_drift():
    assert json.loads(Path('schema/message.schema.json').read_text()) == generated_schema()


@pytest.mark.parametrize('index', range(15))
def test_valid_fixtures(index):
    data = json.loads(Path('tests/fixtures/messages-valid.json').read_text())[index]
    AgentRoomMessageSubmit.model_validate(data)


@pytest.mark.parametrize('index', range(15))
def test_invalid_fixtures(index):
    data = json.loads(Path('tests/fixtures/messages-invalid.json').read_text())[index]
    with pytest.raises(ValidationError):
        AgentRoomMessageSubmit.model_validate(data)
