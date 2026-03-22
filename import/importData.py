import psycopg
import io
import os

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

SOURCE_DSN = f'dbname={SOURCE_DB_DB} user={SOURCE_DB_USER} password={SOURCE_DB_PASSWORD} host={SOURCE_DB_HOST}'
TARGET_DSN = f'dbname={TARGET_DB_DB} user={TARGET_DB_USER} password={TARGET_DB_PASSWORD} host={TARGET_DB_HOST}'

if not SOURCE_DSN or not TARGET_DSN:
    raise RuntimeError("SOURCE_DSN and TARGET_DSN must be set")

BATCH_SIZE = 100_000

FUEL_UUIDS = {
    "diesel": "UUID_FOR_DIESEL",
    "e5": "UUID_FOR_E5",
    "e10": "UUID_FOR_E10"
}

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
        cur.execute("SELECT id, name, postal_code FROM cities")
        return {(name, postal_code): id for id, name, postal_code in cur.fetchall()}


def load_brand_cache(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM brands")
        return {name: id for id, name in cur.fetchall()}


# -----------------------------
# GET OR CREATE HELPERS
# -----------------------------
def get_or_create_city(conn, city_cache, city_name, postal_code):
    key = (city_name, postal_code)

    if key in city_cache:
        return city_cache[key]

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO cities (id, name, postal_code)
            VALUES (gen_random_uuid(), %s, %s)
            RETURNING id
        """, (city_name, postal_code))

        city_id = cur.fetchone()[0]

    conn.commit()
    city_cache[key] = city_id
    return city_id


def get_or_create_brand(conn, brand_cache, brand_name):
    if not brand_name:
        return None

    if brand_name in brand_cache:
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
    return brand_id


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
                get_or_create_city(tgt_conn, city_cache, city_name, postal_code)

    print("Cities migrated")


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
                    lng,
                FROM gas_station
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
                house_number,
                city_id,
                lat,
                lng,
            ))

        with tgt_conn.cursor() as tgt_cur:
            tgt_cur.executemany("""
                INSERT INTO gas_station (
                    id,
                    name,
                    brand_id,
                    street,
                    house_number,
                    city_id,
                    lat,
                    lng,
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


def process_row(row):
    stid, e5, e10, diesel, dt, changed = row

    fuels = {
        "diesel": diesel,
        "e5": e5,
        "e10": e10
    }

    rows = []

    for fuel, price in fuels.items():
        if price is not None and price_changed(changed, fuel):
            rows.append((stid, dt, FUEL_UUIDS[fuel], int(price)))

    return rows


def flush_price_buffer(conn):
    global price_buffer

    if not price_buffer:
        return

    mem = io.StringIO()

    for row in price_buffer:
        mem.write(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\n")

    mem.seek(0)

    with conn.cursor() as cur:
        cur.copy(
            'COPY price_updates (station, "timestamp", fuel_type, price) FROM STDIN',
            mem
        )

    conn.commit()
    print(f"Inserted {len(price_buffer)} prices")

    price_buffer.clear()


# -----------------------------
# MIGRATE PRICES
# -----------------------------
def migrate_prices():
    global price_buffer

    with psycopg.connect(SOURCE_DSN) as src_conn, psycopg.connect(TARGET_DSN) as tgt_conn:
        with src_conn.cursor(name="price_cursor") as src_cur:
            src_cur.itersize = 50_000

            src_cur.execute("""
                SELECT stid, e5, e10, diesel, date, changed
                FROM old_prices
                ORDER BY date
            """)

            for row in src_cur:
                price_buffer.extend(process_row(row))

                if len(price_buffer) >= BATCH_SIZE:
                    flush_price_buffer(tgt_conn)

        flush_price_buffer(tgt_conn)

    print("Prices migrated")


# -----------------------------
# RUN ORDER
# -----------------------------
if __name__ == "__main__":
    migrate_cities()
    migrate_brands()
    migrate_stations()
    migrate_prices()



                





