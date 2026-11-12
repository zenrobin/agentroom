from dataclasses import replace

from app.notify import email_owner


def test_email_requires_starttls_and_never_includes_owner_key(settings, monkeypatch):
    events = []
    class SMTP:
        def __init__(self, host, port, timeout):
            assert host == 'smtp.example.com' and port == 587 and timeout == 4
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def ehlo(self):
            events.append('ehlo')
        def starttls(self, context):
            assert context.check_hostname
            events.append('starttls')
        def login(self, user, password):
            assert events == ['ehlo', 'starttls', 'ehlo']
            events.append('login')
        def send_message(self, message):
            text = message.get_body(preferencelist=('plain',)).get_content()
            html = message.get_body(preferencelist=('html',)).get_content()
            assert '/owner/review#' in text and 'Review request' in html
            assert 'curl ' not in text and settings.owner_key not in text
            assert 'request id' in message['Subject']
            assert 'poll_secret' not in text
            events.append('sent')
    monkeypatch.setattr('app.notify.smtplib.SMTP', SMTP)
    config = replace(settings, email='robin@example.com', smtp_host='smtp.example.com', smtp_user='robin', smtp_pass='example', smtp_from='room@example.com')
    email_owner(config, {'request_id': 'id', 'agent_name': 'example'})
    assert events[-2:] == ['login', 'sent']
