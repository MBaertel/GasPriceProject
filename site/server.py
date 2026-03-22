from uuid import UUID
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from datetime import datetime, timezone, timedelta
import psycopg
import os

DB_CONFIG = {
    os.environ.get("DB_HOST"),
    os.environ.get("DB_PORT"),
    os.environ.get("DB_USER"),
    os.environ.get("DB_PASSWORD"),
    os.environ.get("DB_DB")
}

app = FastAPI()

def get_connection():
    return psycopg.connect(**DB_CONFIG)

@app.get("/cities")
def search_cities(query: str = ""):
    query = """
        SELECT id, name
        FROM cities
        WHERE name ILIKE %s
        ORDER BY name
        LIMIT 30
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, [f"%{query}%"])
            rows = cur.fetchall()

    return [
        {
            "id": row[0],
            "name": row[1],
            "brand": row[2],
            "street": row[4],
            "house_number": row[5]
        }
        for row in rows
    ]

@app.get("/stations")
def search_stations(city_id: str, q: str = ""):
    query = """
        SELECT id, name, street, house_number
        FROM stations
        WHERE city = %s
        AND name ILIKE %s
        ORDER BY name
        LIMIT 20
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, [city_id, f"%{q}%"])
            rows = cur.fetchall()

    return [
        {
            "id": row[0],
            "name": row[1],
            "street": row[2],
            "house_number": row[3]
        }
        for row in rows
    ]


@app.get("/prices")
def get_prices(
    station_id: UUID,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    limit: int = 500
):
    if end_time is None:
        end_time = datetime.now(timezone.utc)

    if start_time is None:
        start_time = end_time - timedelta(days=7)

    query = """
        SELECT station, fuel_type, timestamp, price
        FROM price_updates
        WHERE station = %s
        AND timestamp >= %s
        AND timestamp <= %s
        ORDER BY timestamp
        LIMIT %s
    """

    params = [station_id, start_time, end_time, limit]

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

    return [
        {
            "station_id": row[0],
            "timestamp": row[1],
            "fueltype_id": row[2],
            "price": row[3]
        }
        for row in rows
    ]

app.mount("/",StaticFiles(directory="content",html=True),name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)



