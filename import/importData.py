import psycopg
import io
import os
from dotenv import load_dotenv
import tempfile

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

BATCH_SIZE = 50_000

FUEL_UUIDS = {}

BITMASK = {
    "diesel": 1,
    "e5": 4,
    "e10": 16
}

price_buffer = []

# -----------------------------
# LOOKUP CACHE LOADERS
# -----------------------------
def load_city_cache(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, postal_code FROM cities")
        return {(postal_code): id for id, postal_code in cur.fetchall()}


def load_brand_cache(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM brands")
        return {name: id for id, name in cur.fetchall()}
    
def load_station_cache(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM stations")
        return {row[0] for row in cur.fetchall()}


# -----------------------------
# GET OR CREATE HELPERS
# -----------------------------
def get_or_create_city(conn, city_cache, city_name, postal_code):
    key = postal_code

    if key in city_cache:
        print(f"city {city_name} already exists.")
        return city_cache[key]

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cities (id, name, postal_code)
            VALUES (gen_random_uuid(), %s, %s)
            ON CONFLICT DO NOTHING
            RETURNING id
        """, (city_name, postal_code))

        row = cur.fetchone()  # fetch the row
        if row is None:
            return

        city_id = row[0]

    conn.commit()
    city_cache[key] = city_id
    print(f"created city {city_name} with id {city_id}.")
    return city_id


def get_or_create_brand(conn, brand_cache, brand_name):
    if not brand_name:
        return None

    if brand_name in brand_cache:
        print(f"brand {brand_name} already exists.")
        return brand_cache[brand_name]

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO brands (id, name)
            VALUES (gen_random_uuid(), %s)
            RETURNING id
        """, (brand_name,))

        brand_id = cur.fetchone()[0]

    conn.commit()
    brand_cache[brand_name] = brand_id
    print(f"created brand {brand_name} with id {brand_id}.")
    return brand_id

def get_or_create_fuels(conn):
    fuels = ["diesel", "e5", "e10"]

    with conn.cursor() as cur:
        for fuel in fuels:
            cur.execute("""
                INSERT INTO fuel_types (id,name)
                VALUES (gen_random_uuid(),%s)
                ON CONFLICT (name) DO NOTHING
                RETURNING id
            """, (fuel,))

        cur.execute("""
            SELECT id, name FROM fuel_types
            WHERE name = ANY(%s)
        """, (fuels,))

        fuel_map = {name: fuel_id for fuel_id, name in cur.fetchall()}

    conn.commit()
    return fuel_map


# -----------------------------
# MIGRATE CITIES
# -----------------------------
def migrate_cities():
    with psycopg.connect(SOURCE_DSN) as src_conn, psycopg.connect(TARGET_DSN) as tgt_conn:
        city_cache = load_city_cache(tgt_conn)

        with src_conn.cursor() as src_cur:
            src_cur.execute("""
                SELECT DISTINCT place, post_code
                FROM gas_station
                WHERE place IS NOT NULL
            """)

            for city_name, postal_code in src_cur.fetchall():
                get_or_create_city(tgt_conn, city_cache, normalize_city_name(city_name), postal_code)

    print("Cities migrated")

def normalize_city_name(name: str) -> str:
    return " ".join(word.capitalize() for word in name.split())


# -----------------------------
# MIGRATE BRANDS
# -----------------------------
def migrate_brands():
    with psycopg.connect(SOURCE_DSN) as src_conn, psycopg.connect(TARGET_DSN) as tgt_conn:
        brand_cache = load_brand_cache(tgt_conn)

        with src_conn.cursor() as src_cur:
            src_cur.execute("""
                SELECT DISTINCT brand
                FROM gas_station
                WHERE brand IS NOT NULL
            """)

            for (brand_name,) in src_cur.fetchall():
                get_or_create_brand(tgt_conn, brand_cache, brand_name)

    print("Brands migrated")


# -----------------------------
# MIGRATE STATIONS
# -----------------------------
def migrate_stations():
    with psycopg.connect(SOURCE_DSN) as src_conn, psycopg.connect(TARGET_DSN) as tgt_conn:
        city_cache = load_city_cache(tgt_conn)
        brand_cache = load_brand_cache(tgt_conn)

        with src_conn.cursor() as src_cur:
            src_cur.execute("""
                SELECT 
                    id,
                    name,
                    brand,
                    street,
                    house_number,
                    post_code,
                    place,
                    lat,
                    lng
                FROM gas_station;
            """)

            rows = src_cur.fetchall()

        insert_rows = []

        for row in rows:
            (
                station_id,
                name,
                brand,
                street,
                house_number,
                post_code,
                place,
                lat,
                lng,
            ) = row

            city_id = get_or_create_city(tgt_conn, city_cache, place, post_code)
            brand_id = get_or_create_brand(tgt_conn, brand_cache, brand)

            insert_rows.append((
                station_id,
                name,
                brand_id,
                street,
                house_number or 0,
                city_id,
                lat,
                lng,
            ))

        with tgt_conn.cursor() as tgt_cur:
            tgt_cur.executemany("""
                INSERT INTO stations (
                    id,
                    name,
                    brand,
                    street,
                    house_number,
                    city,
                    latitude,
                    longitude
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING
            """, insert_rows)

        tgt_conn.commit()

    print("Stations migrated")


# -----------------------------
# PRICE HELPERS
# -----------------------------
def price_changed(mask, fuel):
    return (mask & BITMASK[fuel]) != 0


def process_row(row,station_cache):
    stid, e5, e10, diesel, dt, changed = row

    fuels = {
        "diesel": diesel,
        "e5": e5,
        "e10": e10
    }

    rows = []

    for fuel, price in fuels.items():
        if price is not None and price_changed(changed, fuel) and stid in station_cache:
            rows.append((stid, dt, FUEL_UUIDS[fuel], int(price)))

    return rows

def flush_price_buffer(conn):
    global price_buffer
    if not price_buffer:
        return
    try:
        print("Flushing price buffer...")
        def row_generator():
            for station, timestamp, fuel_type, price in price_buffer:
                if hasattr(timestamp, "tzinfo") and timestamp.tzinfo is not None:
                    timestamp = timestamp.replace(tzinfo=None)
                timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S")
                yield f"{station},{timestamp_str},{fuel_type},{price}\n"
        with conn.cursor() as cur:
            with cur.copy('COPY price_updates (station, "timestamp", fuel_type, price) FROM STDIN WITH (FORMAT csv)') as copy:
                for line in row_generator():
                    copy.write(line)
        conn.commit()
    except Exception as e:
        print(e)
    print(f"Inserted {len(price_buffer)} prices")
    price_buffer.clear()


# -----------------------------
# MIGRATE PRICES
# -----------------------------
def migrate_prices():
    global price_buffer

    with psycopg.connect(SOURCE_DSN) as src_conn, psycopg.connect(TARGET_DSN) as tgt_conn:
        station_cache = load_station_cache(tgt_conn)
        with src_conn.cursor(name="price_cursor") as src_cur:
            src_cur.itersize = BATCH_SIZE

            src_cur.execute("""
                SELECT stid, e5, e10, diesel, date, changed
                FROM gas_station_information_history
                ORDER BY date DESC
            """)

            print("Migrating Prices")

            for row in src_cur:
                price_buffer.extend(process_row(row,station_cache))

                if(len(price_buffer) % 1000 == 0):
                    print(len(price_buffer))

                if len(price_buffer) >= BATCH_SIZE:
                    flush_price_buffer(tgt_conn)

        flush_price_buffer(tgt_conn)

    print("Prices migrated")


# -----------------------------
# RUN ORDER
# -----------------------------
if __name__ == "__main__":
    with psycopg.connect(TARGET_DSN) as conn:
        FUEL_UUIDS = get_or_create_fuels(conn)
    migrate_cities()
    migrate_brands()
    migrate_stations()
    migrate_prices()



                





