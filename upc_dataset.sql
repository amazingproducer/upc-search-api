CREATE DATABASE upc_dataset;

\c upc_dataset

CREATE TYPE upc_data_source AS ENUM ('usda', 'uhtt', 'off');

CREATE TABLE dataset_source_meta
(
    source_id                       serial PRIMARY KEY,
    source_name                     upc_data_source,
    refresh_check_url               TEXT NOT NULL,
    current_version_hash            TEXT,
    current_version_url             TEXT,
    current_version_release_name    TEXT,
    current_version_date            DATE NOT NULL,
    last_update_check               DATE
);

CREATE TABLE product_info
(
    entry_id                serial PRIMARY KEY,
    upc                     varchar(13),
    source                  upc_data_source,
    source_entry_date       date NOT NULL,
    name                    text NOT NULL,
    category                text,
    serving_size            numeric,
    serving_unit            text,
    CONSTRAINT check_numeric CHECK (upc ~ '^[0-9]*$'),
    CONSTRAINT check_length CHECK (length(upc) >= 12),
    CONSTRAINT check_unique_composite UNIQUE (upc, source, source_entry_date)
);

INSERT INTO dataset_source_meta VALUES 
(
    'off', 'OpenFoodFacts', 'https://static.openfoodfacts.org/data/openfoodfacts-mongodbdump.tar.gz'
);