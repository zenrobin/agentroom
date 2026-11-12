"""Operator-only rollback preparation. Dry run unless --apply is explicit.

Run with the intended deployment's DATABASE_URL_UNPOOLED (or DATABASE_URL) in
the environment. Restore agent fallback and stop helpers before --apply.
This removes outbound transport records, never identities/messages/cursors.
"""
import argparse
import json
import os

from . import db


def prepare(conn, *, apply=False):
    with db.transaction(conn):
        rows=db.rows(conn,"""SELECT subscription_id FROM webhook_subscriptions WHERE
            (url='' AND secret='') OR EXISTS(SELECT 1 FROM outbound_connectors o
                WHERE o.subscription_id=webhook_subscriptions.subscription_id)""")
        if apply:
            # Serialize with registration and keep admission closed through the
            # promotion window. An old release simply ignores this operator flag.
            conn.execute("INSERT INTO operations_leases VALUES('outbound_disabled',?) ON CONFLICT(name) DO NOTHING",(db.stamp(),))
            conn.execute('DELETE FROM outbound_streams')
            for row in rows:
                sid=row['subscription_id']
                conn.execute('DELETE FROM webhook_deliveries WHERE subscription_id=?',(sid,))
                conn.execute('DELETE FROM webhook_subscriptions WHERE subscription_id=?',(sid,))
            db.audit(conn,'owner','connector.outbound_rollback',detail=str(len(rows)))
        return {'outbound_grants':len(rows),'applied':apply,'preserves':'members, sessions, messages, cursors, processing receipts, other transports'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    action=parser.add_mutually_exclusive_group()
    action.add_argument('--apply',action='store_true')
    action.add_argument('--enable',action='store_true',help='Reopen admission after deliberately returning to a compatible1.7+ build')
    args=parser.parse_args()
    target=os.getenv('DATABASE_URL_UNPOOLED') or os.getenv('DATABASE_URL')
    if not target:
        parser.error('Set the intended deployment database in the environment; no credentials on the command line.')
    conn=db.connect(target)
    try:
        if args.enable:
            with db.transaction(conn):
                conn.execute("DELETE FROM operations_leases WHERE name='outbound_disabled'")
                db.audit(conn,'owner','connector.outbound_enabled')
            print(json.dumps({'outbound_admission':'enabled'}))
        else:
            print(json.dumps(prepare(conn,apply=args.apply)))
    finally:
        conn.close()


if __name__=='__main__':
    main()
