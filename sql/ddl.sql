BEGIN;

DROP TABLE IF EXISTS fact_sales CASCADE;

DROP TABLE IF EXISTS customer CASCADE;
DROP TABLE IF EXISTS pet CASCADE;
DROP TABLE IF EXISTS seller CASCADE;
DROP TABLE IF EXISTS store CASCADE;
DROP TABLE IF EXISTS supplier CASCADE;
DROP TABLE IF EXISTS product CASCADE;

DROP TABLE IF EXISTS city CASCADE;
DROP TABLE IF EXISTS state CASCADE;
DROP TABLE IF EXISTS country CASCADE;

DROP TABLE IF EXISTS pet_breed CASCADE;

DROP TABLE IF EXISTS product_category CASCADE;
DROP TABLE IF EXISTS product_brand CASCADE;
DROP TABLE IF EXISTS product_material CASCADE;
DROP TABLE IF EXISTS product_color CASCADE;
DROP TABLE IF EXISTS product_size CASCADE;
DROP TABLE IF EXISTS pet_category CASCADE;

CREATE TABLE country (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    CONSTRAINT uq_country_name UNIQUE (name)
);

CREATE TABLE state (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    country_id BIGINT NOT NULL REFERENCES country(id),
    CONSTRAINT uq_state_name_country UNIQUE (name, country_id)
);

CREATE TABLE city (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    state_id BIGINT REFERENCES state(id),
    country_id BIGINT NOT NULL REFERENCES country(id),
    CONSTRAINT uq_city_name_state_country UNIQUE (name, state_id, country_id)
);

CREATE TABLE product_category (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    CONSTRAINT uq_product_category_name UNIQUE (name)
);

CREATE TABLE product_brand (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    CONSTRAINT uq_product_brand_name UNIQUE (name)
);

CREATE TABLE product_material (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    CONSTRAINT uq_product_material_name UNIQUE (name)
);

CREATE TABLE product_color (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    CONSTRAINT uq_product_color_name UNIQUE (name)
);

CREATE TABLE product_size (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    CONSTRAINT uq_product_size_name UNIQUE (name)
);

CREATE TABLE pet_category (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    CONSTRAINT uq_pet_category_name UNIQUE (name)
);

CREATE TABLE supplier (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT NOT NULL,
    contact TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city_id BIGINT REFERENCES city(id),
    CONSTRAINT uq_supplier_name_email_phone UNIQUE (name, email, phone)
);

CREATE TABLE product (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    pet_category_id BIGINT REFERENCES pet_category(id),
    category_id BIGINT REFERENCES product_category(id),
    brand_id BIGINT REFERENCES product_brand(id),
    material_id BIGINT REFERENCES product_material(id),
    color_id BIGINT REFERENCES product_color(id),
    size_id BIGINT REFERENCES product_size(id),
    supplier_id BIGINT REFERENCES supplier(id),
    quantity INTEGER,
    weight NUMERIC(12, 3),
    rating NUMERIC(4, 2),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE,
    description TEXT
);

CREATE TABLE pet_breed (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    type_name TEXT NOT NULL,
    breed_name TEXT NOT NULL,
    CONSTRAINT uq_pet_breed_type_breed UNIQUE (type_name, breed_name)
);

CREATE TABLE pet (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT,
    pet_breed_id BIGINT REFERENCES pet_breed(id),
    CONSTRAINT uq_pet_name_breed UNIQUE (name, pet_breed_id)
);

CREATE TABLE customer (
    id BIGINT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    email TEXT,
    postal_code TEXT,
    country_id BIGINT REFERENCES country(id),
    pet_id BIGINT REFERENCES pet(id)
);

CREATE TABLE seller (
    id BIGINT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    postal_code TEXT,
    country_id BIGINT REFERENCES country(id)
);

CREATE TABLE store (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name TEXT,
    location TEXT,
    phone TEXT,
    email TEXT,
    city_id BIGINT REFERENCES city(id),
    state_id BIGINT REFERENCES state(id),
    country_id BIGINT REFERENCES country(id),
    CONSTRAINT uq_store_name_location_city UNIQUE (name, location, city_id)
);

CREATE TABLE fact_sales (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sale_date DATE NOT NULL,
    customer_id BIGINT NOT NULL REFERENCES customer(id),
    seller_id BIGINT NOT NULL REFERENCES seller(id),
    product_id BIGINT NOT NULL REFERENCES product(id),
    store_id BIGINT NOT NULL REFERENCES store(id),
    quantity INTEGER NOT NULL,
    total_price NUMERIC(14, 2) NOT NULL,
    unit_price NUMERIC(14, 2)
);

COMMIT;