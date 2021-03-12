#!/usr/bin/env python3
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import NamedTupleCursor as ds_cur
import requests
import pymongo

import re
import csv
from string import hexdigits
from urllib.parse import unquote
from html.parser import HTMLParser
from datetime import date as d
from datetime import datetime as dt
import subprocess
from os import getenv

### subversion check
from sys import version_info as version
if version < (3, 6):
    print("Python version 3.6 or greater required.")
    quit()

upc_DATABASE_KEY = getenv('upc_DATABASE_KEY')

## Setup database if it's empty
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

### get dataset_source_meta table
ds_meta = None
db_conn = psycopg2.connect(user='barcodeserver', host='10.8.0.55', password=upc_DATABASE_KEY, dbname='upc_data')
with db_conn.cursor(cursor_factory=ds_cur) as db_cur:
    db_cur.execute('SELECT * FROM dataset_source_meta')
    ds_meta = [row._asdict() for row in db_cur]

db_conn.close()
print(f"Dataset Source Metadata:\n{ds_meta}")

### GET INFO ABOUT OPENFOODFACTS DATA
def is_hexadecimal(string):
    "Check each character in a string against the hexadecimal character set."
    return all(char in set(hexdigits) for char in string)


off_current_hash = None
off_update_hash = None
off_current_version_url = None
off_update_hash_url = None
for i in ds_meta:
    if i['source_name'] == 'off':
        off_current_hash = i["current_version_hash"]
        off_update_hash_url = i["refresh_check_url"]
        off_current_version_url = i["current_version_url"]

try:
    r = requests.get(off_update_hash_url)
    off_update_hash = r.text.split(" ")[0]
    if len(off_update_hash) != 64 or not is_hexadecimal(off_update_hash):
        print("Retrieved OFF update checksum is not a SHA-256 hash.")
        off_update_hash = None
    print("OFF update checksum retrieval succeeded.")
except requests.exceptions.RequestException as e:
    print("OFF update checksum retrieval failed.", e)
    off_update_hash = None

# if off_update_hash:
#     if not off_current_hash or off_current_hash != off_update_hash:
#         sb_off_start = dt.now()
#         off_sp = subprocess.run("./get_OFF_update.sh")
#         if off_sp.returncode == 0:
#             print("OpenFoodFacts Data Update Acquired.")
#         else:
#             print(f"OpenFoodFacts Data Update Failed (exit code {off_sp.returncode}).")
#         print(f"Elapsed time: {dt.now() - sb_off_start}")


### Use PyMongo to access retrieved OpenFoodFacts data
m_client = pymongo.MongoClient()
m_db = m_client['off_temp']
off_collection = m_db['product_info']
m_dataset = off_collection.find({})
row_count = off_collection.estimated_document_count()
count = 0

m_fields = ['_id', 'code', 'product_name', 'categories_tags', 'created_t', 'created_datetime', 'last_modified_t', 'last_modified_datetime', 'serving_size']
db_fields = ['source', 'source_item_id', 'upc', 'name', 'category', 'db_entry_date', 'source_item_submission_date', 'source_item_publication_date', 'serving_size_fulltext']
db_mapping = {'source':'off', 'source_item_id':'_id', 'upc':'code', 'name':'product_name', 'category':'categories_tags', 'db_entry_date':None, 'source_item_submission_date':None, 'source_item_publication_date':None, 'serving_size_fulltext':'serving_size'}

def validate_upc(code):
    p_EAN = re.compile('\d{13}$')
    p_UPC = re.compile('\d{12}$')
    if code == None:
        return None
    if p_EAN.search(str(code)):
        u_match = p_EAN.search(str(code)).group()
    elif p_UPC.search(str(code)):
        u_match = p_UPC.search(str(code)).group()
    else:
        return None
    if len(str(int(str(code)))) < 11:
        print(len(str(int(str(code)))))
        return None
    if p_UPC.match(u_match):
        u_match = "0"+u_match
    return u_match

print(validate_upc('0000000018517'))

### Upsert OpenFoodFacts entries
def upsert_off_entry(entry):
    db_conn = psycopg2.connect(user='barcodeserver', host='10.8.0.55', password=upc_DATABASE_KEY, dbname='upc_data')
    db_conn.autocommit = True
    with db_conn.cursor() as db_cur:
        db_cur.execute(f"""
        INSERT INTO
        product_info ({', '.join(db_fields)})
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        serving_size_fulltext = EXCLUDED.serving_size_fulltext
        WHERE
        EXCLUDED.source_item_publication_date > product_info.source_item_publication_date;
        """,
        (entry['source'], entry['source_item_id'], entry['upc'], entry['name'], entry['category'], entry['db_entry_date'], entry['source_item_submission_date'], entry['source_item_publication_date'], entry['serving_size_fulltext'])
        )
        print(db_cur.query.decode('ascii'))
        print(db_cur.fetchall())
    db_conn.close()

start_time = dt.now()
for m_d in m_dataset:
    count += 1
    m_entry = {}
    entry = {}
    kill_flag = False
    entry['source'] = 'off'
    entry['db_entry_date'] = d.strftime(d.today(), '%Y-%m-%d')
    if not count % 1000:
        current_time = dt.now()
        print(f"Completed {count} out of {row_count} rows, {current_time - start_time} elapsed.")
    for i in m_fields:
        if i in m_d.keys():
            m_entry[i] = m_d[i]
    if 'product_name' not in m_entry.keys():
        print("Product name failure")
        kill_flag = True
    elif not m_entry['product_name']:
        print("Product name absent")
        kill_flag = True
    if validate_upc(m_entry['code']) == None:
        print("UPC failure")
        kill_flag = True
    else:
        m_entry['code'] = validate_upc(m_entry['code'])
        entry['upc'] = validate_upc(m_entry['code'])
    if "categories_tags" in m_entry.keys():
        if m_entry['categories_tags'] == None:
            entry['category'] = m_entry['categories_tags']
    else:
        entry['category'] = None
    if "serving_size" in m_entry.keys():
        if m_entry['serving_size'] == None:
            entry['serving_size_fulltext'] = m_entry["serving_size"]
    else:
        entry['serving_size_fulltext'] = None
    if 'created_t' in m_entry.keys():
        if 'created_datetime' in m_entry.keys():
            m_entry.pop('created_datetime', None)
        entry['source_item_submission_date'] = d.fromtimestamp(m_entry['created_t'])
#            entry['source_item_submission_date'] = d.strftime(d.fromtimestamp(m_entry['created_t']), '%Y-%m-%d')
    else:
        entry['source_item_submission_date'] = d.fromisoformat(m_entry['created_datetime'])
#            entry['source_item_submission_date'] = d.strftime(d.fromisoformat(m_entry['created_datetime']), '%Y-%m-%d')
        if 'created_datetime' in m_entry.keys():
            m_entry.pop('created_t', None)
        else:
            print("Submission date failure")
            kill_flag = True
    if 'last_modified_t' in m_entry.keys():
        if 'last_modified_datetime' in m_entry.keys():
            m_entry.pop('last_modified_datetime', None)
        entry['source_item_publication_date'] = d.strftime(d.fromtimestamp(m_entry['last_modified_t']), '%Y-%m-%d')
    else:
        entry['source_item_publication_date'] = d.strftime(d.fromisoformat(m_entry['last_modified_datetime']), '%Y-%m-%d')
        if 'last_modified_datetime' in m_entry.keys():
            m_entry.pop('last_modified_t', None)
        else:
            print("Publication date failure")
            kill_flag = True
    if kill_flag:
        print(f"Kill flag set for {m_entry['_id']}")
    else:
        for db_field in db_fields:
            if db_field not in entry.keys():
                entry[db_field] = m_entry[db_mapping[db_field]]
        print(entry)
        upsert_off_entry(entry)


### Update metadata after OFF update
db_conn = psycopg2.connect(user='barcodeserver', host='10.8.0.55', password=upc_DATABASE_KEY, dbname='upc_data')
db_conn.autocommit = True
with db_conn.cursor() as db_cur:
    db_cur.execute("""
    UPDATE dataset_source_meta
    SET current_version_date = %s,
    current_version_hash = %s,
    last_update_check = %s
    WHERE
    source_name = %s;
    """,
    (d.today(), off_update_hash, d.today(), 'off')
    )
db_conn.close()

## GET INFO ABOUT USDA DATA:
### Check datasource meta table for USDA data attributes
usda_current_version_date = None
usda_dataset_index_url = None
usda_dataset_index_raw = None
for i in ds_meta:
    if i['source_name'] == 'usda':
        usda_current_version_date = i['current_version_date']
        usda_dataset_index_url = i["refresh_check_url"]
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

if usda_current_version_date == None or latest_date > usda_current_version_date:
    print("USDA dataset update available.")

## grab the latest archive and extract it. 
#import subprocess

usda_sp = subprocess.run(["./get_USDA_update.sh", latest_url])
if usda_sp.returncode == 0:
    print("USDA Data Update Acquired.")
else:
    print(f"USDA Data Update Failed (exit code {usda_sp.returncode}).")

### process acquired USDA files
# TODO: update the dataset_source_meta table with new source details

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
        f_upc = validate_upc(row["gtin_upc"])
        f_cat = [[row["branded_food_category"]], None][not row["branded_food_category"]]
        f_ss = [row["serving_size"], None][not row["serving_size"]]
        f_ssu = [row["serving_size_unit"], None][not row["serving_size_unit"]]
        f_sd = row["available_date"]
        count += 1
        if not count % 1000:
            current_time = dt.now()
            print(f"Completed {count} out of {row_count} rows, {current_time - start_time} elapsed.")
        if f_upc:
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
                        db_conn.commit()
                    db_conn.close()

                    food_data.append({"source_item_id":f_id, "upc":f_upc, "name":f_pn, "category":f_cat, "db_entry_date":d.today(), "source_item_submission_date":f_sd, "source_item_publication_date":f_pd, "serving_size":f_ss, "serving_size_unit":f_ssu})
                    break
    end_time = dt.now()
    print(f"Elapsed time: {end_time - start_time}")
subprocess.run("./cleanup_USDA_update.sh")

### Update metadata after USDA update
db_conn = psycopg2.connect(user='barcodeserver', host='10.8.0.55', password=upc_DATABASE_KEY, dbname='upc_data')
db_conn.autocommit = True
with db_conn.cursor() as db_cur:
    db_cur.execute("""
    UPDATE dataset_source_meta
    SET current_version_url = %s,
    last_update_check = %s
    WHERE
    source_name = = %s;
    """,
    (d.today(), d.today(), 'usda')
    )
db_conn.close()


### save the joined data to a csv in case we want it
with open('newfile.csv', 'w') as newfile:
    nd_w = csv.DictWriter(newfile, fieldnames=fieldnames)
    nd_w.writeheader()
    nd_w.writerows(food_data)

