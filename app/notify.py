import asyncio
import hashlib
import hmac
import logging
import smtplib
import ssl
import time
from email.message import EmailMessage
from html import escape

import httpx

from . import db, review
from .signing import canonical_json

log = logging.getLogger('agent_room')


def email_content(settings, summary):
    rid = summary['request_id']
    name = summary.get('display_name') or summary['agent_name']
    # Full request IDs give each application its own subject, even for similarly named agents.
    subject = f'[{settings.room_name}] {summary["agent_name"]} wants to join — request {rid}'
    subject = subject.replace('\r', ' ').replace('\n', ' ')
    link = review.link(settings, rid)
    fields = [('Agent name', summary['agent_name']), ('Display name', name),
              ('Human owner', summary.get('owner_name', '')), ('Contact', summary.get('owner_contact', '')),
              ('Platform', summary.get('platform', '')), ('Purpose', summary.get('purpose', '')),
              ('Request ID', rid)]
    text = f'{name} is asking to join {settings.room_name}.\n\n'
    text += '\n'.join(f'{label}: {value}' for label, value in fields)
    text += f'\n\nReview this request in your browser:\n{link}\n\n'
    text += 'Opening the link does not approve the agent. Review the identity, then choose Approve or Decline.\n'
    text += 'This private link expires after 24 hours and can review only this application. Do not forward it.\n'
    rows = ''.join('<tr><td style="padding:10px 0;color:#60756c;font-size:13px;vertical-align:top;width:115px">' + escape(label) +
                   '</td><td style="padding:10px 0;overflow-wrap:anywhere;white-space:pre-wrap">' + escape(str(value)) + '</td></tr>' for label, value in fields)
    html = ('<!doctype html><html><body style="margin:0;padding:32px 16px;background:#f2f5f4;font-family:Arial,sans-serif;color:#172b35">'
            '<div style="max-width:580px;margin:auto"><p style="color:#225c52;font-weight:bold">' + escape(settings.room_name) + '</p>'
            '<div style="background:#fff;border:1px solid #dbe4df;border-radius:16px;padding:28px">'
            '<p style="color:#56746a;font-size:12px;letter-spacing:1px">MEMBERSHIP REQUEST</p>'
            '<h1 style="font-size:25px;line-height:1.3">' + escape(name) + ' wants to join</h1>'
            '<p style="line-height:1.6;color:#51635d">Review the agent and its human before granting access to your room.</p>'
            '<table role="presentation" style="width:100%;border-collapse:collapse;font-size:14px">' + rows + '</table>'
            '<p style="margin:28px 0 12px"><a href="' + escape(link, quote=True) + '" style="display:inline-block;background:#195e49;color:#fff;'
            'padding:14px 24px;border-radius:8px;text-decoration:none;font-weight:bold">Review request</a></p>'
            '<p style="font-size:13px;line-height:1.5;color:#60756c">You will choose Approve or Decline on the review page. Opening this email or its link grants no access.</p>'
            '</div><p style="font-size:12px;line-height:1.6;color:#73867b">This private link expires after 24 hours and only reviews this application. Keep this email private.</p></div></body></html>')
    return {'subject': subject, 'text': text, 'html': html}


def email_owner(settings, summary):
    message = EmailMessage()
    message['From'], message['To'] = settings.smtp_from, settings.email
    content = email_content(settings, summary)
    message['Subject'] = content['subject']
    message.set_content(content['text'])
    message.add_alternative(content['html'], subtype='html')
    with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=4) as smtp:
        smtp.ehlo()
        smtp.starttls(context=ssl.create_default_context())
        smtp.ehlo()
        smtp.login(settings.smtp_user, settings.smtp_pass)
        smtp.send_message(message)


async def retry_channel(settings, summary, channel):
    # Reuse exactly the same signed link/payload throughout retries for provider idempotency.
    content = email_content(settings, summary) if channel == 'resend' else None
    for attempt in range(4):
        try:
            if channel == 'webhook':
                body = canonical_json({'event': 'join_request.created', 'request': summary}).encode()
                timestamp = str(int(time.time()))
                signature = hmac.new(settings.webhook_secret.encode(), timestamp.encode() + b'.' + body, hashlib.sha256).hexdigest()
                async with httpx.AsyncClient(timeout=4, follow_redirects=False) as client:
                    response = await client.post(settings.webhook_url, content=body,
                        headers={'Content-Type': 'application/json', 'X-AR-Timestamp': timestamp, 'X-AR-Signature': 'sha256=' + signature})
                    response.raise_for_status()
            elif channel == 'resend':
                async with httpx.AsyncClient(timeout=4, follow_redirects=False) as client:
                    response = await client.post('https://api.resend.com/emails',
                        headers={'Authorization': 'Bearer ' + settings.resend_api_key,
                                 'Idempotency-Key': 'agent-room-join-review-v2-' + summary.get('_delivery_id', summary['request_id'])},
                        json={'from': settings.resend_from, 'to': [settings.email],
                              **content})
                    response.raise_for_status()
            else:
                await asyncio.to_thread(email_owner, settings, summary)
            try:
                conn = db.connect(db.target(settings))
                try:
                    db.audit(conn, 'room', 'notification.sent', summary['request_id'], detail=channel)
                finally:
                    conn.close()
            except Exception:
                log.warning('notification_audit_failed', extra={'channel': channel})
            return
        except Exception:
            # No exception text: transport exceptions can contain URLs or credentials.
            log.warning('notification_failed', extra={'channel': channel, 'attempt': attempt + 1})
            conn = db.connect(db.target(settings))
            try:
                db.audit(conn, 'room', 'notification.failed', summary['request_id'], detail=f'{channel}:attempt={attempt + 1}')
            finally:
                conn.close()
            if attempt < 3:
                await asyncio.sleep(2 ** attempt)


async def notify_owner(settings, summary):
    channels = []
    if settings.webhook_url:
        channels.append(retry_channel(settings, summary, 'webhook'))
    if settings.resend_api_key and settings.email:
        channels.append(retry_channel(settings, summary, 'resend'))
    elif settings.email:
        channels.append(retry_channel(settings, summary, 'email'))
    await asyncio.gather(*channels)


async def send_owner_login(settings, token):
    """Send only to the configured owner; no caller-supplied recipient or redirect."""
    link = settings.public_url + '/owner#login=' + token
    subject = '[' + settings.room_name.replace('\r', '').replace('\n', '') + '] Owner sign-in'
    text = ('Sign in to your Agent Room owner console:\n\n' + link +
            '\n\nThis private link expires in 15 minutes and works once. Open it, then choose Sign in. '
            'If you did not request this, ignore the email. Do not forward the link.')
    content = {'subject': subject, 'text': text,
               'html': '<p>Sign in to your Agent Room owner console.</p><p><a href="' + escape(link, quote=True) +
                       '">Open owner console</a></p><p>This private link expires in 15 minutes and works once. '
                       'Opening it does not sign you in until you confirm. Do not forward it.</p>'}
    def smtp():
        message = EmailMessage()
        message['From'], message['To'], message['Subject'] = settings.smtp_from, settings.email, subject
        message.set_content(text)
        message.add_alternative(content['html'], subtype='html')
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port, timeout=4) as connection:
            connection.starttls(context=ssl.create_default_context())
            connection.login(settings.smtp_user, settings.smtp_pass)
            connection.send_message(message)
    for attempt in range(3):
        try:
            if settings.resend_api_key:
                async with httpx.AsyncClient(timeout=4, follow_redirects=False) as client:
                    result = await client.post('https://api.resend.com/emails',
                        headers={'Authorization': 'Bearer ' + settings.resend_api_key,
                                 'Idempotency-Key': 'ar-owner-login-' + hashlib.sha256(token.encode()).hexdigest()},
                        json={'from': settings.resend_from, 'to': [settings.email], **content})
                    result.raise_for_status()
            else:
                await asyncio.to_thread(smtp)
            return
        except Exception:
            log.warning('owner_login_email_failed', extra={'attempt': attempt + 1})
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
