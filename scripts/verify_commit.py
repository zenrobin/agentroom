#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.signing import verify

parser = argparse.ArgumentParser(description='Verify an exported Agent Room commit against a pinned or supplied public key')
parser.add_argument('file')
parser.add_argument('--public-key', help='Trusted pinned base64 Ed25519 public key (recommended)')
args = parser.parse_args()
data = json.loads(Path(args.file).read_text())
verify(data['commit_message'], args.public_key or data['public_key'])
print(json.dumps({'verified': True, 'negotiation_id': data['commit_message']['body']['negotiation_id']}))
