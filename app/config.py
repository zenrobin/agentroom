import base64
import os
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit


def decode_key(value, name, minimum=32, exact=False):
    try:
        if not re.fullmatch(r'[A-Za-z0-9_+/=-]+', value):
            raise ValueError()
        raw = base64.b64decode(value + '=' * (-len(value) % 4), altchars=b'-_', validate=True)
        if len(raw) < minimum or (exact and len(raw) != minimum):
            raise ValueError()
        return raw
    except (ValueError, TypeError):
        raise ValueError(f'{name} must encode {"exactly" if exact else "at least"} {minimum} random bytes') from None


@dataclass(frozen=True)
class Settings:
    domain: str
    owner_key: str
    jwt_keys: dict
    signing_key: bytes
    db_path: Path
    room_name: str = 'Agent Room'
    room_code: str | None = None
    webhook_url: str | None = None
    webhook_secret: str | None = None
    email: str | None = None
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_user: str | None = None
    smtp_pass: str | None = None
    smtp_from: str | None = None
    max_members: int = 20
    retention_days: int = 30
    dev: bool = False
    caddy: bool = True
    database_url: str | None = None
    database_url_unpooled: str | None = None
    resend_api_key: str | None = None
    resend_from: str = 'Agent Room <onboarding@resend.dev>'
    serverless: bool = False
    cron_secret: str | None = None
    profile: str = 'private'
    public_domain: str | None = None

    @property
    def public_url(self):
        return 'https://' + (self.public_domain or self.domain)

    @classmethod
    def from_env(cls):
        def required(name):
            value = os.getenv(name, '')
            if not value:
                raise ValueError(f'{name} is required')
            return value

        def boolean(name, default):
            value = os.getenv(name, default).lower()
            if value not in {'true', 'false', 'on', 'off'}:
                raise ValueError(f'{name} must be true/false or on/off')
            return value in {'true', 'on'}

        def positive(name, default):
            try:
                number = int(os.getenv(name, str(default)))
                if number <= 0:
                    raise ValueError()
                return number
            except ValueError:
                raise ValueError(f'{name} must be a positive integer') from None

        domain = (os.getenv('AR_DOMAIN') or os.getenv('VERCEL_PROJECT_PRODUCTION_URL') or required('AR_DOMAIN')).lower()
        if len(domain) > 253 or not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?', domain):
            raise ValueError('AR_DOMAIN must be a hostname without scheme, path, or port')
        if any(not label or len(label) > 63 or label.startswith('-') or label.endswith('-') for label in domain.split('.')):
            raise ValueError('AR_DOMAIN has an invalid DNS label')
        owner_key = required('AR_OWNER_KEY')
        if not re.fullmatch(r'[A-Za-z0-9_-]+={0,2}', owner_key):
            raise ValueError('AR_OWNER_KEY must be base64url')
        decode_key(owner_key, 'AR_OWNER_KEY')
        keys = {}
        for entry in required('AR_JWT_KEYS').split(','):
            kid, sep, key = entry.partition(':')
            if not sep or not re.fullmatch(r'[A-Za-z0-9_-]{1,64}', kid) or kid in keys:
                raise ValueError('AR_JWT_KEYS must contain distinct kid:base64 entries')
            keys[kid] = decode_key(key, 'AR_JWT_KEYS', exact=True)
        signing_key = decode_key(required('AR_SIGNING_KEY'), 'AR_SIGNING_KEY', exact=True)
        profile = os.getenv('AR_PROFILE', 'private')
        if profile not in {'private', 'home-team', 'sandbox'}:
            raise ValueError('AR_PROFILE must be private, home-team, or sandbox')
        public_domain = os.getenv('AR_PUBLIC_DOMAIN') or domain
        if not re.fullmatch(r'[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?', public_domain) or any(
                not label or len(label) > 63 or label.startswith('-') or label.endswith('-') for label in public_domain.split('.')):
            raise ValueError('AR_PUBLIC_DOMAIN must be a hostname without scheme, path, or port')
        webhook = os.getenv('AR_OWNER_WEBHOOK_URL') or None
        secret = os.getenv('AR_WEBHOOK_SECRET') or None
        dev = boolean('AR_DEV', 'false')
        if os.getenv('AR_CADDY', 'on') not in {'on', 'off'}:
            raise ValueError('AR_CADDY must be on or off')
        if bool(webhook) != bool(secret):
            raise ValueError('AR_OWNER_WEBHOOK_URL and AR_WEBHOOK_SECRET must be configured together')
        if webhook:
            url = urlsplit(webhook)
            if url.scheme not in ({'http', 'https'} if dev else {'https'}) or not url.hostname or url.username or url.password or url.fragment:
                raise ValueError('AR_OWNER_WEBHOOK_URL must be an HTTPS URL without credentials or fragment')
        names = ['AR_OWNER_EMAIL', 'AR_SMTP_HOST', 'AR_SMTP_USER', 'AR_SMTP_PASS', 'AR_SMTP_FROM']
        email_values = [os.getenv(n) or None for n in names]
        resend_key = os.getenv('RESEND_API_KEY') or None
        if any(email_values) and not all(email_values) and not (resend_key and email_values[0] and not any(email_values[1:])):
            raise ValueError('Configure all of ' + ', '.join(names))
        if profile != 'sandbox' and not webhook and not all(email_values) and not (resend_key and email_values[0]):
            raise ValueError('At least one complete notification channel (webhook or email) is required')
        if profile == 'sandbox' and (webhook or resend_key or any(email_values)):
            raise ValueError('Sandbox must not have email or owner webhook credentials')
        if any(v and ('\n' in v or '\r' in v) for v in [email_values[0], email_values[4]]):
            raise ValueError('Email addresses must not contain newlines')
        path = Path(os.getenv('AR_DB_PATH', '/data/room.db')).expanduser().absolute()
        database_url = os.getenv('DATABASE_URL') or None
        serverless = os.getenv('VERCEL') == '1'
        if serverless and not database_url:
            raise ValueError('DATABASE_URL is required on Vercel; local SQLite is never used there')
        if database_url and urlsplit(database_url).scheme not in {'postgres', 'postgresql'}:
            raise ValueError('DATABASE_URL must be a PostgreSQL connection URL')
        return cls(domain, owner_key, keys, signing_key, path,
                   room_name=os.getenv('AR_ROOM_NAME', {'home-team': 'Home Team', 'sandbox': 'AgentRoom Sandbox'}.get(profile, 'AgentRoom')), room_code=os.getenv('AR_ROOM_CODE') or None,
                   webhook_url=webhook, webhook_secret=secret, email=email_values[0], smtp_host=email_values[1],
                   smtp_user=email_values[2], smtp_pass=email_values[3], smtp_from=email_values[4],
                   smtp_port=positive('AR_SMTP_PORT', 587), max_members=positive('AR_MAX_MEMBERS', 20),
                   retention_days=positive('AR_RETENTION_DAYS', 30), dev=dev, caddy=boolean('AR_CADDY', 'on'),
                   database_url=database_url, resend_api_key=resend_key,
                   database_url_unpooled=os.getenv('DATABASE_URL_UNPOOLED') or None,
                   resend_from=os.getenv('AR_RESEND_FROM', 'Agent Room <onboarding@resend.dev>'),
                   serverless=serverless, cron_secret=os.getenv('CRON_SECRET') or None,
                   profile=profile, public_domain=public_domain)
