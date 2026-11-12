"""Expiring, single-application owner review capabilities, separate from member and owner keys."""
import hashlib
import hmac
from datetime import timedelta
from urllib.parse import urlencode

import jwt

from . import db
from .errors import APIError


def key(settings):
    return hmac.new(settings.owner_key.encode(), b'agent-room-owner-review-v1', hashlib.sha256).digest()


def issue(settings, request_id):
    now = db.utcnow()
    return jwt.encode({'iss': 'agent-room-review', 'aud': settings.domain,
                       'sub': request_id, 'typ': 'owner_review', 'iat': int(now.timestamp()),
                       'exp': int((now + timedelta(hours=24)).timestamp())}, key(settings), algorithm='HS256')


def verify(settings, request_id, token):
    try:
        claims = jwt.decode(token, key(settings), algorithms=['HS256'], audience=settings.domain,
                            issuer='agent-room-review', options={'require': ['sub', 'typ', 'iat', 'exp']})
        if claims['sub'] != request_id or claims['typ'] != 'owner_review':
            raise ValueError()
    except (jwt.PyJWTError, ValueError, TypeError):
        raise APIError(401, 'review_link_invalid', 'This review link is invalid or expired. Ask the room owner for a new review email.') from None


def link(settings, request_id):
    # Fragments stay in the browser, outside HTTP request URLs, access logs and Referer headers.
    return settings.public_url + '/owner/review#' + urlencode({'request': request_id, 'token': issue(settings, request_id)})
