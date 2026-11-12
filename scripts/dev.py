"""Run a loopback-only fictional sandbox; never load a product environment."""
import argparse
import os
from pathlib import Path
import subprocess
import sys

from dotenv import dotenv_values
from generate_keys import write_keys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--port', type=int, default=8000)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error('Choose a port between 1 and 65535')
    if any(os.getenv(key) for key in ('DATABASE_URL', 'DATABASE_URL_UNPOOLED', 'VERCEL')):
        raise SystemExit('Refusing a cloud/database environment. Open a clean local shell for this sandbox.')
    root = Path(__file__).resolve().parents[1]
    keyfile = root / '.env.dev.local'
    if not keyfile.exists():
        write_keys(keyfile)
    os.chmod(keyfile, 0o600)
    keys = dotenv_values(keyfile)
    expected = {'AR_OWNER_KEY', 'AR_JWT_KEYS', 'AR_SIGNING_KEY', 'CRON_SECRET'}
    if set(keys) != expected or not all(keys.values()):
        raise SystemExit('Unexpected development key file. Keep product settings out of .env.dev.local.')
    env = {k: v for k, v in os.environ.items() if not k.startswith(('AR_', 'VERCEL', 'DATABASE_', 'RESEND_')) and k != 'CRON_SECRET'}
    env.update(keys)
    env.update(AR_DOMAIN='localhost', AR_PUBLIC_DOMAIN='localhost', AR_PROFILE='sandbox', AR_DEV='true',
               AR_CADDY='off', AR_ROOM_NAME='AgentRoom Local Sandbox', AR_DB_PATH=str(root/'data/local-sandbox.db'))
    print(f'Fictional sandbox: http://127.0.0.1:{args.port} (Ctrl-C to stop)', flush=True)
    try:
        return subprocess.call([sys.executable, '-m', 'uvicorn', 'app.main:app', '--host', '127.0.0.1',
                                '--port', str(args.port)], cwd=root, env=env)
    except KeyboardInterrupt:
        return 0


if __name__ == '__main__':
    raise SystemExit(main())
