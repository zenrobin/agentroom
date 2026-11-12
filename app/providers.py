"""Provider-specific wake authentication. These credentials never authorize room access."""
import base64
import hashlib
import hmac
import re

import httpx
from cryptography.fernet import Fernet

from .errors import APIError

GROK_DOCS = 'https://docs.x.ai/grok-bot/skills-routines-and-automations#webhooks'
MUSE_DOCS = 'https://www.meta.com/help/artificial-intelligence/1484325780075655/'


def cipher(settings):
    key = hmac.new(settings.signing_key, b'agent-room-provider-credentials-v1', hashlib.sha256).digest()
    return Fernet(base64.urlsafe_b64encode(key))


def grok_credentials(value, authorization):
    """Constrain bearer credentials to the routine endpoint observed in Grok's desktop UI."""
    url = httpx.URL(value)
    if (url.scheme != 'https' or url.host != 'api2.cursor.sh' or url.port not in {None, 443}
            or url.userinfo or url.query or url.fragment
            or not re.fullmatch(r'/automations/webhook/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', url.path)):
        raise APIError(422, 'invalid_grok_routine', 'Copy the POST to address from your Grok routine. Expected https://api2.cursor.sh/automations/webhook/ followed by its routine ID.')
    token = authorization.strip()
    if token.lower().startswith('authorization:'):
        token = token[len('authorization:'):].strip()
    if token.lower().startswith('bearer '):
        token = token[7:]
    if not re.fullmatch(r'[A-Za-z0-9._~+/=-]{16,2048}', token) or token.startswith('arwh_'):
        raise APIError(422, 'invalid_grok_key', 'Paste the routine key or its copied header. Use the key from Grok’s routine details.')
    return 'Bearer ' + token


def catalog():
    return [
        {'id': 'grok', 'name': 'Grok Bot', 'delivery': 'managed_direct', 'optional_delivery': 'scheduled',
         'automatic_wake': True, 'human_step': 'Approve the agent and native routine. Link its address/key privately once; reuse a configured connection. Scheduled setup is also available.',
         'guide': '/setup/grok', 'documentation': GROK_DOCS},
        {'id': 'muse', 'name': 'Muse', 'delivery': 'outbound_pilot', 'optional_delivery': 'scheduled', 'automatic_wake': False,
         'human_step': 'Approve the agent. It verifies its local wake hook and installs the outbound connector; keep scheduled recovery until live testing passes.',
         'guide': '/setup/muse', 'documentation': MUSE_DOCS},
        {'id': 'agentroom', 'name': 'Other agent / custom runtime', 'delivery': 'signed_webhook_or_poll',
         'automatic_wake': False, 'guide': '/runtime-guide'},
    ]
