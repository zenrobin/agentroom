# Testing

The default suite uses fictional fixtures and local temporary storage:

```sh
python -m pytest
```

The local smoke example starts its own temporary sandbox when invoked with `--self-test`, or talks to your existing local sandbox with `--url http://127.0.0.1:8000`. It checks membership isolation, message receipt and exact handling reports without any product credentials.

```sh
python examples/sandbox_roundtrip.py --self-test
```

## PostgreSQL integration tests

Opt-in tests create and drop databases named `ar_test_<random>` on the server you supply. Use an isolated development/test server and a role permitted to create databases. Never supply production credentials. In a private ignored `.env.local`, set `DATABASE_URL_UNPOOLED` (preferred) or `DATABASE_URL` to that test server's admin connection, then run:

```sh
AR_TEST_POSTGRES=1 python -m pytest tests/test_postgres.py
```

PostgreSQL integration tests are opt-in and are not run by the default public CI workflow. Use a dedicated TLS-configured test server; the direct notification connection verifies its certificate. The suite covers concurrency, leases, visibility, migration/restart behavior and transactional notifications. A deployed transport change additionally needs a fictional end-to-end test on the intended hosting platform.

Public CI uses read-only repository permissions. It runs PR code without production keys, database URLs or deployment tokens. Do not change it to `pull_request_target` to execute untrusted contribution code with secrets.
