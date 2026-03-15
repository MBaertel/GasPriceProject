CREATE TABLE "stations" (
  "station_id" uuid PRIMARY KEY NOT NULL,
  "name" varchar(64) NOT NULL,
  "brand" uuid,
  "city" uuid NOT NULL,
  "street" varchar(128) NOT NULL,
  "house_number" varchar(32) NOT NULL,
  "latitude" decimal,
  "longitude" decimal
);

CREATE TABLE "cities" (
  "city_id" uuid PRIMARY KEY NOT NULL,
  "postal_code" varchar(16) NOT NULL,
  "name" varchar(64) NOT NULL
);

CREATE TABLE "brands" (
  "brand_id" uuid PRIMARY KEY NOT NULL,
  "name" varchar(64) NOT NULL
);

CREATE TABLE "fuel_types" (
  "type_id" uuid PRIMARY KEY NOT NULL,
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

COMMENT ON COLUMN "price_updates"."price" IS 'in millicent';

ALTER TABLE "stations" ADD FOREIGN KEY ("brand") REFERENCES "brands" ("brand_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "station_fuel_types" ADD FOREIGN KEY ("station") REFERENCES "stations" ("station_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "station_fuel_types" ADD FOREIGN KEY ("fuel_type") REFERENCES "fuel_types" ("type_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "stations" ADD FOREIGN KEY ("city") REFERENCES "cities" ("city_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "price_updates" ADD FOREIGN KEY ("station") REFERENCES "stations" ("station_id") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "price_updates" ADD FOREIGN KEY ("fuel_type") REFERENCES "fuel_types" ("type_id") DEFERRABLE INITIALLY IMMEDIATE;
