#!/usr/bin/env python3
"""Apply the checked-in schema to the linked database. Never print its URL."""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import db

load_dotenv(Path(__file__).resolve().parents[1] / '.env.local')
url = os.getenv('DATABASE_URL_UNPOOLED') or os.getenv('DATABASE_URL')
if not url:
    raise SystemExit('DATABASE_URL_UNPOOLED or DATABASE_URL is required')
db.initialize(url)
print('PostgreSQL schema ready (version 3).')
