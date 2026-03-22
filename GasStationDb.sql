CREATE TABLE "stations" (
  "id" uuid PRIMARY KEY NOT NULL,
  "name" varchar(128) NOT NULL,
  "brand" uuid,
  "city" uuid NOT NULL,
  "street" varchar(128) NOT NULL,
  "house_number" varchar(32) NOT NULL,
  "latitude" decimal,
  "longitude" decimal
);
CREATE TABLE "cities" (
  "id" uuid PRIMARY KEY NOT NULL,
  "postal_code" varchar(16) NOT NULL,
  "name" varchar(255) NOT NULL
);
CREATE TABLE "brands" (
  "id" uuid PRIMARY KEY NOT NULL,
  "name" varchar(128) NOT NULL
);
CREATE TABLE "fuel_types" (
  "id" uuid PRIMARY KEY NOT NULL,
  "name" varchar(32) NOT NULL
);
CREATE TABLE "station_fuel_types" (
  "station" uuid NOT NULL,
  "fuel_type" uuid NOT NULL,
  PRIMARY KEY ("station", "fuel_type")
);
CREATE TABLE "price_updates" (
  "station" uuid NOT NULL,
  "timestamp" timestamp NOT NULL,
  "fuel_type" uuid NOT NULL,
  "price" int NOT NULL,
  PRIMARY KEY ("station", "timestamp", "fuel_type")
);
SELECT create_hypertable(
    'price_updates',
    'timestamp',
    chunk_time_interval => INTERVAL '1 month'
  );
COMMENT ON COLUMN "price_updates"."price" IS 'in millicent';
ALTER TABLE "stations"
ADD FOREIGN KEY ("brand") REFERENCES "brands" ("id") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "station_fuel_types"
ADD FOREIGN KEY ("station") REFERENCES "stations" ("id") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "station_fuel_types"
ADD FOREIGN KEY ("fuel_type") REFERENCES "fuel_types" ("id") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "stations"
ADD FOREIGN KEY ("city") REFERENCES "cities" ("id") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "price_updates"
ADD FOREIGN KEY ("station") REFERENCES "stations" ("id") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "price_updates"
ADD FOREIGN KEY ("fuel_type") REFERENCES "fuel_types" ("id") DEFERRABLE INITIALLY IMMEDIATE;
CREATE INDEX idx_stations_city ON stations(city);
CREATE INDEX idx_price_updates_station_timestamp ON price_updates (station, "timestamp" DESC);
CREATE INDEX idx_price_updates_fuel_timestamp ON price_updates (fuel_type, "timestamp" DESC);
CREATE INDEX idx_stations_brand ON stations(brand);
CREATE INDEX idx_stations_name ON stations(name);
CREATE UNIQUE INDEX idx_cities_name_postal ON cities(name, postal_code);