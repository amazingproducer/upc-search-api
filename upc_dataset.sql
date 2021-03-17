CREATE TYPE upc_data_source AS ENUM ('usda', 'uhtt', 'off');

CREATE TABLE dataset_source_meta
(
    id                              serial PRIMARY KEY,
    source_name                     upc_data_source,
    refresh_check_url               TEXT NOT NULL,
    current_version_hash            TEXT,
    current_version_url             TEXT,
    current_version_release_name    TEXT,
    current_version_date            DATE,
    last_update_check               DATE
);

CREATE TABLE product_info
(
    id                              serial PRIMARY KEY,
    source                          upc_data_source,
    source_item_id                  text,
    upc                             varchar(14) NOT NULL,
    name                            text NOT NULL,
    category                        text[],
    db_entry_date                   date NOT NULL,
    source_item_submission_date     date,
    source_item_publication_date    date,
    serving_size                    numeric,
    serving_size_unit               text,
    serving_size_fulltext           text,
    CONSTRAINT check_numeric CHECK (upc ~ '^[0-9]*$'),
    CONSTRAINT check_length CHECK (length(upc) >= 12),
    CONSTRAINT check_unique_composite UNIQUE (upc, source)
);

CREATE INDEX idx_upc ON product_info (upc);
CREATE INDEX idx_product_info ON product_info (source, upc, source_item_publication_date);

INSERT INTO dataset_source_meta (source_name, refresh_check_url, current_version_url) VALUES 
(
    'off', 'https://static.openfoodfacts.org/data/sha256sum', 'https://static.openfoodfacts.org/data/openfoodfacts-mongodbdump.tar.gz'
),
(
    'usda', 'https://fdc.nal.usda.gov/fdc-datasets/', NULL
),
(
    'uhtt', 'https://api.github.com/repos/papyrussolution/UhttBarcodeReference/releases', NULL
);