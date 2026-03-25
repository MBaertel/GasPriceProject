import psycopg
import io
import os
from dotenv import load_dotenv

load_dotenv()

SOURCE_DB_HOST = os.environ.get("SOURCE_DB_HOST")
SOURCE_DB_PORT = os.environ.get("SOURCE_DB_PORT")
SOURCE_DB_USER = os.environ.get("SOURCE_DB_USER")
SOURCE_DB_PASSWORD = os.environ.get("SOURCE_DB_PASSWORD")
SOURCE_DB_DB = os.environ.get("SOURCE_DB_DB")

TARGET_DB_HOST = os.environ.get("TARGET_DB_HOST")
TARGET_DB_PORT = os.environ.get("TARGET_DB_PORT")
TARGET_DB_USER = os.environ.get("TARGET_DB_USER")
TARGET_DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
TARGET_DB_DB = os.environ.get("TARGET_DB_DB")

SOURCE_DSN = f'dbname={SOURCE_DB_DB} user={SOURCE_DB_USER} password={SOURCE_DB_PASSWORD} host={SOURCE_DB_HOST} port={SOURCE_DB_PORT}'
TARGET_DSN = f'dbname={TARGET_DB_DB} user={TARGET_DB_USER} password={TARGET_DB_PASSWORD} host={TARGET_DB_HOST} port={TARGET_DB_PORT}'

if not SOURCE_DSN or not TARGET_DSN:
    raise RuntimeError("SOURCE_DSN and TARGET_DSN must be set")

BATCH_SIZE = 10_000

FUEL_UUIDS = {}

BITMASK = {
    "diesel": 1,
    "e5": 4,
    "e10": 16
}

with psycopg.connect(TARGET_DSN) as conn:
    mem = io.StringIO()
    mem.write("51d4b6e4-a095-1aa0-e100-80009459e03a\t2026-01-24 12:15:28\t5eb2058a-b375-4f1a-b772-58656537f522\t1699\n")
    mem.seek(0)

    with conn.cursor() as cur:
        cur.copy(
            'COPY price_updates (station, "timestamp", fuel_type, price) FROM STDIN WITH (FORMAT text)',
            mem
        )
    conn.commit()

