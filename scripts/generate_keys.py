"""Generate new-room keys in a private local file. Never rotate a live room implicitly."""
import base64
import os
from pathlib import Path
import secrets


def new_keys():
    return {
        'AR_OWNER_KEY': secrets.token_urlsafe(32),
        'AR_JWT_KEYS': 'k1:' + base64.b64encode(secrets.token_bytes(32)).decode(),
        'AR_SIGNING_KEY': base64.b64encode(secrets.token_bytes(32)).decode(),
        'CRON_SECRET': secrets.token_urlsafe(32),
    }


def write_keys(path):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        stream.write(''.join(f'{key}={value}\n' for key, value in new_keys().items()))
        stream.flush()
        os.fsync(stream.fileno())


if __name__ == '__main__':
    path = Path(__file__).resolve().parents[1] / '.env.generated.local'
    try:
        write_keys(path)
    except FileExistsError:
        raise SystemExit('Existing key file retained. Use it for its intended new room; do not overwrite live keys.')
    print('Saved .env.generated.local with private permissions. No keys printed.')
