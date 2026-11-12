"""Fictional sandbox example: private delivery, durable receipt, exact handling report."""
import argparse
from contextlib import contextmanager
from pathlib import Path
import secrets
import sys
from tempfile import TemporaryDirectory
from uuid import uuid4

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import db


@contextmanager
def target(args):
    if args.self_test:
        from fastapi.testclient import TestClient
        from app.config import Settings
        from app.main import create_app
        with TemporaryDirectory() as folder:
            settings = Settings(domain='localhost', owner_key=secrets.token_urlsafe(32),
                jwt_keys={'demo': secrets.token_bytes(32)}, signing_key=secrets.token_bytes(32),
                db_path=Path(folder)/'demo.db', profile='sandbox', dev=True)
            with TestClient(create_app(settings)) as client:
                yield client
    else:
        if args.url.rstrip('/') not in {'http://127.0.0.1:8000', 'http://localhost:8000'}:
            raise SystemExit('This example is restricted to the local sandbox on port 8000.')
        with httpx.Client(base_url=args.url, timeout=15) as client:
            yield client


def run(client):
    def call(method, path, **kwargs):
        result = client.request(method, path, **kwargs)
        result.raise_for_status()
        return result.json()
    assert call('GET', '/v1/room')['profile'] == 'sandbox'
    def join():
        pair = call('POST', '/v1/sandbox/join', json={'request_id': str(uuid4()), 'poll_secret': secrets.token_urlsafe(32)})
        return pair, {'Authorization': 'Bearer '+pair['access_token']}
    first, headers = join()
    _, other = join()
    mid = str(uuid4())
    message = {'id': mid, 'nonce': secrets.token_urlsafe(16), 'ts': db.stamp(), 'type': 'chat',
               'thread_id': mid, 'to': [first['peer_id']], 'body': {'text': 'Fictional round-trip example'}}
    call('POST', '/v1/messages', headers=headers, json=message)
    assert not call('GET', '/v1/messages', headers=other)['items'], 'Private lane isolation failed'
    inbox = call('GET', '/v1/inbox', headers=headers)
    assert inbox['items'], 'Expected the simulator reply'
    # A real adapter persists to its durable inbox before ack. This example writes
    # a private temporary file so the ordering is concrete, then handles a no-op.
    import json, os
    with TemporaryDirectory() as folder:
        path = Path(folder)/'inbox.json'
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(inbox['items'], stream); stream.flush(); os.fsync(stream.fileno())
        call('POST', '/v1/inbox/ack', headers=headers, json={'seq': inbox['items'][-1]['seq']})
        receipts = [{'seq': m['seq'], 'message_id': m['id'], 'outcome': 'handled'} for m in json.loads(path.read_text())]
        # Handling consists only of checking the fictional simulator's reply.
        assert all(m['body'].get('text') for m in inbox['items'])
        call('POST', '/v1/connector/receipts', headers=headers, json={'receipts': receipts})
    print('PASS: private lane, simulator reply, durable save, acknowledgement, exact handling report.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--self-test', action='store_true')
    parser.add_argument('--url', default='http://127.0.0.1:8000')
    with target(parser.parse_args()) as client:
        run(client)
