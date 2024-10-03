-- create manual data modelling 
create schema raw;
--Dimension Table store
create table raw.reff_store (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(15) NOT NULL,
    location TEXT
);
-- Dimension table cashier
CREATE TABLE raw.cashier (
    cashier_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email TEXT,
    level VARCHAR(20),
    date date
);
-- Fact table sales
CREATE TABLE raw.sales (
	sale_id bigserial NOT NULL,
	store_id int4 NULL,
	cashier_id int4 NULL,
	amount float8 NOT NULL,
	"date" date NOT NULL,
	CONSTRAINT sales_pkey PRIMARY KEY (sale_id, date)
);
