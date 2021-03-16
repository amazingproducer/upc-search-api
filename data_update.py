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
from datetime import timedelta as td
import subprocess
from os import getenv

### subversion check
from sys import version_info as version
if version < (3, 6):
    print("Python version 3.6 or greater required.")
    quit()

upc_DATABASE_KEY = getenv('upc_DATABASE_KEY')
update_interval = td(days=30)

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
            # DEBUG
#            print(db_cur.query.decode('utf-8'))
            db_conn.commit()

db_conn.close()

### get dataset_source_meta table
ds_meta = None
db_conn = psycopg2.connect(user='barcodeserver', host='10.8.0.55', password=upc_DATABASE_KEY, dbname='upc_data')
with db_conn.cursor(cursor_factory=ds_cur) as db_cur:
    db_cur.execute('SELECT * FROM dataset_source_meta')
    ds_meta = [row._asdict() for row in db_cur]

db_conn.close()
#print(f"Dataset Source Metadata:\n{ds_meta}")


def validate_upc(code):
    p_EAN = re.compile('\d{13}$')
    p_UPC = re.compile('\d{12}$')
    if code == None:
        return None
    if not code:
        return None
    if p_EAN.search(str(code)):
        u_match = p_EAN.search(str(code)).group()
    elif p_UPC.search(str(code)):
        u_match = p_UPC.search(str(code)).group()
    else:
        return None
    if len(str(int(str(code)))) < 11:
        return None
    if p_UPC.match(u_match):
        u_match = "0"+u_match
    return u_match


## GET INFO ABOUT UHTT DATA
uhtt_current_release = None
uhtt_current_date = None
uhtt_last_check_date = None
uhtt_refresh_check_url = None
for i in ds_meta:
    if i['source_name'] == 'uhtt':
        uhtt_current_release = i['current_version_release_name']
        uhtt_current_version_url = i['current_version_url']
        uhtt_current_date = i['current_version_date']
        uhtt_last_check_date = i['last_update_check']
        uhtt_refresh_check_url = i['refresh_check_url']
try:
    u_r = requests.get(uhtt_refresh_check_url).json()
except requests.exceptions.RequestException as e:
    print("UHTT update check failed.", e)

def upsert_uhtt_entry(entry):
    db_fields = ['source', 'source_item_id', 'upc', 'name', 'db_entry_date', 'source_item_publication_date']
    db_conn = psycopg2.connect(user='barcodeserver', host='10.8.0.55', password=upc_DATABASE_KEY, dbname='upc_data')
    db_conn.autocommit = True
    with db_conn.cursor() as db_cur:
        db_cur.execute(f"""
        INSERT INTO
        product_info ({', '.join(db_fields)})
        VALUES
        (%s, %s, %s, %s, %s, %s)
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
        (entry['source'], entry['source_item_id'], entry['upc'], entry['name'], entry['db_entry_date'], entry['source_item_publication_date'])
        )
#        print(db_cur.query.decode('utf-8'))
    db_conn.close()

u_update_required = False
if not uhtt_current_date or d.fromisoformat(u_r[0]['published_at'].split('T')[0]) > d.fromisoformat(uhtt_current_date):
    uhtt_current_date = d.fromisoformat(u_r[0]['published_at'].split('T')[0])
    uhtt_current_release = u_r[0]['tag_name']
    uhtt_last_check_date = d.today()
    for i in u_r[0]['assets']:
        if i['browser_download_url'].endswith('.7z'):
            uhtt_current_version_url = i['browser_download_url']
            u_update_required = True
if u_update_required:
    sp_u_start = dt.now()
    u_sp = subprocess.run("./get_UHTT_update.sh")
    if u_sp.returncode == 0:
        print("UHTT Data Update Acquired.")
    else:
        print(f"UHTT Data Update Failed (exit code {u_sp.returncode}).")
    print(f"Elapsed time: {dt.now() - sp_u_start}")

    ### Process Acquired Source
    u_row_count = 0
    count = 0
    kill_count = 0
    with open('uhtt_barcode_ref_all.csv', 'r') as u_file:
        u_row_count = sum(1 for lin in u_file)
    with open('uhtt_barcode_ref_all.csv', 'r') as u_file:
        u_start_time = dt.now()
        u_dict = csv.DictReader(u_file, delimiter='\t')
        # chz = 25
        for row in u_dict:
            # chz -= 1
            # if chz < 1:
            #     break
            count += 1
            entry = {}
            entry['upc'] = validate_upc(row['UPCEAN'])
            entry['name'] = row['Name']
            if entry['upc'] and entry['name']:
                entry['source'] = 'uhtt'
                entry['source_item_id'] = row['ID']
                entry['db_entry_date'] = d.today()
                entry['source_item_publication_date'] = uhtt_current_date
                upsert_uhtt_entry(entry)
                if not count % 1000:
                    current_time = dt.now()
                    print(f"Completed {count} out of {u_row_count} rows, rejecting {killcount}, {current_time - u_start_time} elapsed.")
            else:
                print(f"Rejected: {entry['upc']}, {entry['name']}.")
                kill_count += 1
        print(f"UHTT upsert complete. Total Time Elapsed: {dt.now() - u_start_time}")
    ### Update metadata after OFF update
    db_conn = psycopg2.connect(user='barcodeserver', host='10.8.0.55', password=upc_DATABASE_KEY, dbname='upc_data')
    db_conn.autocommit = True
    with db_conn.cursor() as db_cur:
        db_cur.execute("""
        UPDATE dataset_source_meta
        SET current_version_date = %s,
        current_version_release_name = %s,
        current_version_url = %s,
        last_update_check = %s
        WHERE
        source_name = %s;
        """,
        (uhtt_current_date, uhtt_current_release, uhtt_current_version_url, d.today(), 'uhtt')
        )
    db_conn.close()


## GET INFO ABOUT OPENFOODFACTS DATA
def is_hexadecimal(string):
    "Check each character in a string against the hexadecimal character set."
    return all(char in set(hexdigits) for char in string)


off_current_hash = None
off_update_hash = None
off_current_version_url = None
off_update_hash_url = None
off_last_check_date = None
off_update_required = False
for i in ds_meta:
    if i['source_name'] == 'off':
        off_current_hash = i["current_version_hash"]
        off_update_hash_url = i["refresh_check_url"]
        off_current_version_url = i["current_version_url"]
        off_last_check_date = i['last_update_check']
        off_current_version_date = i['current_version_date']

# print(f"Current Version Date: {off_current_version_date}")
# print(f"Last Check Date: {off_last_check_date}")
# print(f"Current Version Age: {(d.today() - off_current_version_date).days}, {type((d.today() - off_current_version_date).days)}")
# print(f"Minimum Version Age: {update_interval.days}, {type(update_interval.days)}")
# print(f"Updated in last 30 days: {(d.today() - off_current_version_date) > (update_interval).days}")

if not off_last_check_date:
    print('OFF update_required')
    off_update_required = True
else:
    update_age = d.today() - off_current_version_date
    if update_age.days > update_interval.days:
        off_update_required = True
        print("OFF update required")
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
if off_update_hash and off_update_required:
    if not off_current_hash or off_current_hash != off_update_hash:
        sp_off_start = dt.now()
        off_sp = subprocess.run("./get_OFF_update.sh")
        if off_sp.returncode == 0:
            print("OpenFoodFacts Data Update Acquired.")
        else:
            print(f"OpenFoodFacts Data Update Failed (exit code {off_sp.returncode}).")
        print(f"Elapsed time: {dt.now() - sp_off_start}")



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
#        print(db_cur.query.decode('utf-8'))
    db_conn.close()

## Use PyMongo to access retrieved OpenFoodFacts data
m_client = pymongo.MongoClient()
m_db = m_client['off_temp']
off_collection = m_db['product_info']
m_dataset = off_collection.find({})
row_count = off_collection.estimated_document_count()
count = 0
kill_count = 0

m_fields = ['_id', 'code', 'product_name', 'categories_tags', 'created_t', 'created_datetime', 'last_modified_t', 'last_modified_datetime', 'serving_size']
db_fields = ['source', 'source_item_id', 'upc', 'name', 'category', 'db_entry_date', 'source_item_submission_date', 'source_item_publication_date', 'serving_size_fulltext']
db_mapping = {'source':'off', 'source_item_id':'_id', 'upc':'code', 'name':'product_name', 'category':'categories_tags', 'db_entry_date':None, 'source_item_submission_date':None, 'source_item_publication_date':None, 'serving_size_fulltext':'serving_size'}

### process and upsert OpenFoodfacts data
if off_update_required == True:
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
            print(f"Processed {count} out of {row_count} rows, rejecting {kill_count} rows, {current_time - start_time} elapsed.")
        for i in m_fields:
            if i in m_d.keys():
                m_entry[i] = m_d[i]
        if 'product_name' not in m_entry.keys():
    #        print("Product name failure")
            kill_flag = True
        elif not m_entry['product_name']:
    #        print("Product name absent")
            kill_flag = True
        if validate_upc(m_entry['code']) == None:
    #        print("UPC failure")
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
            if m_entry['created_t']:
                entry['source_item_submission_date'] = d.fromtimestamp(m_entry['created_t'])
                if 'created_datetime' in m_entry.keys():
                    m_entry.pop('created_datetime', None)   
            else:
                m_entry.pop('created_t', None)
                if 'created_datetime' in m_entry.keys():
                    if m_entry['created_datetime']:
                        entry['source_item_submission_date'] = d.fromisoformat(m_entry['created_datetime'])
                    else:
                        m_entry.pop('created_datetime', None)
                        kill_flag = True
    #                    print('Submission date failure')
        if 'last_modified_t' in m_entry.keys():
            if m_entry['last_modified_t']:
                entry['source_item_publication_date'] = d.fromtimestamp(m_entry['last_modified_t'])
                if 'last_modified_datetime' in m_entry.keys():
                    m_entry.pop('last_modified_datetime', None)
            else:
                m_entry.pop('last_modified_t', None)
                if 'last_modified_datetime' in m_entry.keys():
                    if m_entry['last_modified_datetime']:
                        entry['source_item_publication_date'] = d.fromisoformat(m_entry['last_modified_datetime'])
                    else:
                        m_entry.pop('last_modified_datetime', None)
    #                    print('Last Modified date failure')
                        kill_flag = True
        if kill_flag:
            kill_count += 1
        else:
            for db_field in db_fields:
                if db_field not in entry.keys():
                    entry[db_field] = m_entry[db_mapping[db_field]]
    #        print(entry)
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

# GET INFO ABOUT USDA DATA:
## Check datasource meta table for USDA data attributes
usda_current_version_date = None
usda_dataset_index_url = None
usda_dataset_index_raw = None
usda_update_required = False
for i in ds_meta:
    if i['source_name'] == 'usda':
        usda_current_version_date = i['current_version_date']
        usda_dataset_index_url = i["refresh_check_url"]
        usda_dataset_index_raw = requests.get(usda_dataset_index_url).text
        usda_current_version_url = i['current_version_url']

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
usda_latest_date = None
usda_latest_url = None
for i in USDAIndexParser.dataset_list:
    i_formatted = unquote(i).replace(" ", "")
    date_string = i_formatted.strip(".zip").strip("FoodData_Central_csv")
    date_object = d.fromisoformat(date_string)
    if usda_latest_date == None or date_object > usda_latest_date:
        usda_latest_date = date_object
        usda_latest_url = usda_dataset_index_url + i

if usda_current_version_date == None or usda_latest_date > usda_current_version_date:
    print("USDA dataset update available.")
    usda_update_required = True

## grab the latest archive and extract it. 
if usda_update_required:
    usda_sp = subprocess.run(["./get_USDA_update.sh", usda_latest_url])
    if usda_sp.returncode == 0:
        print("USDA Data Update Acquired.")
    else:
        print(f"USDA Data Update Failed (exit code {usda_sp.returncode}).")

### process acquired USDA files
if usda_update_required:
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

                        food_data.append({"source_item_id":f_id, "upc":f_upc, "name":f_pn, "category":f_cat, "db_entry_date":d.today(), "source_item_submission_date":f_sd, "source_item_publication_date":f_pd, "serving_size":f_ss, "serving_size_unit":f_ssu})
                        break
        end_time = dt.now()
        print(f"Elapsed time: {end_time - start_time}")
    db_conn.close()
    subprocess.run("./cleanup_USDA_update.sh")

    ### Update metadata after USDA update
    db_conn = psycopg2.connect(user='barcodeserver', host='10.8.0.55', password=upc_DATABASE_KEY, dbname='upc_data')
    db_conn.autocommit = True
    with db_conn.cursor() as db_cur:
        db_cur.execute("""
        UPDATE dataset_source_meta
        SET current_version_date = %s,
        current_version_url = %s,
        last_update_check = %s
        WHERE
        source_name = %s;
        """,
        (usda_latest_date, usda_latest_url, d.today(), 'usda')
        )
    db_conn.close()


    ## save the joined data to a csv in case we want it
    with open('newfile.csv', 'w') as newfile:
        nd_w = csv.DictWriter(newfile, fieldnames=fieldnames)
        nd_w.writeheader()
        nd_w.writerows(food_data)

