#!/usr/bin/env python3
#import SQLAlchemy
import psycopg2
import requests
#import PyMongo

#import csv
from string import hexdigits
from urllib.parse import unquote
from html.parser import HTMLParser
from datetime import datetime as dt

### subversion check
from sys import version_info as version

if version < (3, 6):
    print("Python version 3.6 or greater required.")
    quit()

### GET INFO ABOUT OPENFOODFACTS DATA
# How you gonna compare against something you haven't examined?

# To determine if a string is hexadecimal:
# is it better to check that each character is a subset of the hexadecimal character set?
# is it better to simply try converting the string into an integer?
# def is_hexadecimal(string):
#     "Check each character in a string against the hexadecimal character set."
#     return all(char in set(hexdigits) for char in string)

# off_current_hash = "some string i haven't collected"
# off_update_hash_url = "https://static.openfoodfacts.org/data/sha256sum"

# def off_retrieve_current_checksum():

# def off_retrieve_update_checksum():
#     try:
#         r = requests.get(off_update_hash_url)
#         off_update_hash = r.text.split(" ")[0]
#         if len(off_update_hash) != 64 or not is_hexadecimal(off_update_hash):
#             print("Retrieved OFF update checksum is not a SHA-256 hash.")
#             return None
#         print("OFF update checksum retrieval succeeded.")
#         return off_update_hash
#     except requests.exceptions.RequestException as e:
#         print("OFF update checksum retrieval failed.", e)
#         return None

### sqlalchemy basics
# from sqlalchemy import create_engine

# from os import getenv
# upc_DATABASE_KEY = getenv('upc_DATABASE_KEY')
# engine = create_engine(f'postgresql://barcodeserver:{upc_DATABASE_KEY}@10.0.8.55/upc_dataset')




# ### psycopg2 basics
import psycopg2

from os import getenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import NamedTupleCursor as ds_cur
upc_DATABASE_KEY = getenv('upc_DATABASE_KEY')

connection = None
try: 
    db_conn = psycopg2.connect(user='barcodeserver', host='10.8.0.55', password=upc_DATABASE_KEY, dbname='upc_data')
except:
    print('DB connection failed.')
    db_conn = psycopg2.connect(user='barcodeserver', host='10.8.0.55', password=upc_DATABASE_KEY, dbname='postgres')
    db_conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with db_conn.cursor() as db_cur:
        db_cur.execute('CREATE DATABASE upc_data')
    db_conn.close()
    db_conn = psycopg2.connect(user='barcodeserver', host='10.8.0.55', password=upc_DATABASE_KEY, dbname='upc_data')
    with db_conn.cursor() as db_cur:
        with open('upc_dataset.sql', 'r') as sqlfile:
            db_cur.execute(sqlfile.read())
            db_conn.commit()

db_conn.close()

### psycopg2 get dataset_source_meta table
ds_meta = None
db_conn = psycopg2.connect(user='barcodeserver', host='10.8.0.55', password=upc_DATABASE_KEY, dbname='upc_data')
with db_conn.cursor(cursor_factory=ds_cur) as db_cur:
    db_cur.execute('SELECT * FROM dataset_source_meta')
    ds_meta = [row._asdict() for row in db_cur]

db_conn.close()
print(f"Dataset Source Metadata:\n{ds_meta}")

## GET INFO ABOUT USDA DATA:
import requests
from urllib.parse import unquote
from html.parser import HTMLParser
from datetime import datetime as dt
usda_dataset_index_url = "https://fdc.nal.usda.gov/fdc-datasets/"
usda_dataset_index_raw = requests.get(usda_dataset_index_url).text

class USDAIndexParser(HTMLParser):
    dataset_list = []
    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href":
                    if "FoodData_Central_csv" in value:
                        USDAIndexParser.dataset_list.append(value)

usda_parser = USDAIndexParser()
usda_parser.feed(usda_dataset_index_raw)
latest_date = None
latest_url = None
for i in USDAIndexParser.dataset_list:
    i_formatted = unquote(i).replace(" ", "")
    date_string = i_formatted.strip(".zip").strip("FoodData_Central_csv")
    date_object = dt.strptime(date_string, "%Y-%m-%d")
    if latest_date == None or date_object > latest_date:
        latest_date = date_object
        latest_url = usda_dataset_index_url + i

### Check datasource meta table for USDA data attributes
for i in ds_meta:
    if i['source_name'] == 'off':
        usda_current_version_date = i['current_version_date']

if usda_current_version_date == None or latest_date > usda_current_version_date:
    print("USDA dataset update available.")

## grab the latest archive and extract it. 
import subprocess

usda_sp = subprocess.run(["./get_USDA_update.sh", latest_url])
if usda_sp.returncode == 0:
    print("USDA Data Update Acquired.")
else:
    print(f"USDA Data Update Failed (exit code {sp.returncode}).")


### process acquired USDA files
import csv
from datetime import date as d
from datetime import datetime as dt
import psycopg2

fn_file = open('food.csv', 'r')
fn = csv.DictReader(fn_file)

bf_file = open('branded_food.csv', 'r')
bf = csv.DictReader(bf_file)

food_names = []
food_data = []
row_count = None    

fieldnames = [
    "source",
    "source_item_id",
    "upc",
    "name",
    "category",
    "db_entry_date",
    "source_item_submission_date",
    "source_item_publication_date",
    "serving_size",
    "serving_size_unit"
]

db_conn = psycopg2.connect(user='barcodeserver', host='10.8.0.55', password=upc_DATABASE_KEY, dbname='upc_data')
db_conn.autocommit = True

with open('food.csv', 'r') as fn_file:
    fn = csv.DictReader(fn_file)
    for row in fn:
        food_names.append({"fdc_id":row["fdc_id"], "product_name":row["description"], "publication_date":row["publication_date"]})

with open('branded_food.csv', 'r') as bf_file:
    row_count = sum(1 for row in bf_file)

with open('branded_food.csv', 'r') as bf_file:
    bf = csv.DictReader(bf_file)
    count = 0
    start_time = dt.now()
    for row in bf:
        f_id = row["fdc_id"]
        f_upc = str(int(row["gtin_upc"]))
        f_cat = [row["branded_food_category"], "NULL"][not row["branded_food_category"]]
        f_ss = row["serving_size"]
        f_ssu = row["serving_size_unit"]
        f_sd = row["available_date"]
        count += 1
        if not count % 1000:
            current_time = dt.now()
            print(f"Completed {count} out of {row_count} rows, {current_time - start_time} elapsed.")
        if 12 <= len(f_upc) <= 14:
#            print(f_upc, type(f_upc), len(f_upc))
            for entry in food_names:
                if entry["fdc_id"] == f_id:
                    f_pn = entry["product_name"]
                    f_pd = entry["publication_date"]
                    with db_conn.cursor() as db_cur:
                        db_cur.execute(f"""
                        INSERT INTO
                        product_info ({', '.join(fieldnames)})
                        VALUES
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT ON CONSTRAINT
                        check_unique_composite
                        DO
                        UPDATE SET
                        source_item_id = EXCLUDED.source_item_id,
                        name = EXCLUDED.name,
                        category = EXCLUDED.category,
                        db_entry_date = EXCLUDED.db_entry_date,
                        source_item_submission_date = EXCLUDED.source_item_submission_date,
                        source_item_publication_date = EXCLUDED.source_item_publication_date,
                        serving_size = EXCLUDED.serving_size,
                        serving_size_unit = EXCLUDED.serving_size_unit
                        WHERE
                        EXCLUDED.source_item_publication_date > product_info.source_item_publication_date;
                        """,
                        ('usda', f_id, f_upc, f_pn, f_cat, d.today(), f_sd, f_pd, f_ss, f_ssu)
                        )

                        # ('usda', '{f_id}', '{f_upc}', '{f_pn}', '{f_cat}', '{d.today()}', '{f_sd}', '{f_pd}', '{f_ss}', '{f_ssu}')


                        # db_cur.execute(f"""INSERT INTO product_info ({', '.join(fieldnames)}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                        # ('usda', f_id, f_upc, f_pn, f_cat, d.today(), f_sd, f_pd, f_ss, f_ssu))

                        # db_cur.execute(f"""INSERT INTO product_info ({', '.join(fieldnames)}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        # ON CONFLICT (upc, source, source_item_publication_date) DO UPDATE
                        # SET source_item_id=%s, name=%s, category=%s, db_entry_date=%s, source_item_submission_date=%s, source_item_publication_date=%s, serving_size=%s, serving_size_unit=%s
                        # WHERE source_item_publication_date > EXCLUDED.source_item_publication_date
                        # ;""",
                        # ('usda', f_id, f_upc, f_pn, f_cat, d.today(), f_sd, f_pd, f_ss, f_ssu))
                        db_conn.commit()
                    food_data.append({"source_item_id":f_id, "upc":f_upc, "name":f_pn, "category":f_cat, "db_entry_date":d.today(), "source_item_submission_date":f_sd, "source_item_publication_date":f_pd, "serving_size":f_ss, "serving_size_unit":f_ssu})
                    break
    end_time = dt.now()
    print(f"Elapsed time: {end_time - start_time}")

### save the joined data to a csv in case we want it
with open('newfile.csv', 'w') as newfile:
    nd_w = csv.DictWriter(newfile, fieldnames=fieldnames)
    nd_w.writeheader()
    nd_w.writerows(food_data)

