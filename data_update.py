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


### sqlalchemy basics
# from sqlalchemy import create_engine

# from os import getenv
# upc_DATABASE_KEY = getenv('upc_DATABASE_KEY')
# engine = create_engine(f'postgresql://barcodeserver:{upc_DATABASE_KEY}@10.0.8.55/upc_dataset')




# ### psycopg2 basics
import psycopg2

from os import getenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
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

### check for a database:
# if connection is not None:
#     connection.autocommit = True
#     cur = connection.cursor()
#     cur.execute("SELECT datname FROM pg_database;")
#     list_database = cur.fetchall()
#     database_name = "upc_dataset"
#     if (database_name,) in list_database:
#         print("Database found.")
#     else:
#         print("Database not found.")
#     connection.close()
#     print('Done')


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








## GET INFO ABOUT USDA DATA:
import requests
from urlllib.parse import unquote
from html.parser import HTMLParser
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
        latest_url = i
print(f"Retain and compare the following URL with the URL retained during the last update: {latest_url}")

## grab the latest archive and extract it. 
import subprocess

USDA_DATASET_URL = "https://fdc.nal.usda.gov/fdc-datasets/FoodData_Central_csv_2020-10-30.zip"
usda_sp = subprocess.run(["./get_USDA_update.sh", USDA_DATASET_URL])
if usda_sp.returncode == 0:
    print("USDA Data Update Acquired.")
else:
    print(f"USDA Data Update Failed (exit code {sp.returncode}).")


### process acquired USDA files for later updating
# turn files into objects for later insertion
import csv
from datetime import datetime as dt

fn_file = open('food.csv', 'r')
fn = csv.DictReader(fn_file)

bf_file = open('branded_food.csv', 'r')
bf = csv.DictReader(bf_file)

food_names = []
food_data = []
row_count = None

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
        f_upc = row["gtin_upc"]
        f_cat = row["branded_food_category"]
        f_ss = row["serving_size"]
        f_ssu = row["serving_size_unit"]
        f_sd = row["available_date"]
        count += 1
        if not count % 1000:
            current_time = dt.now()
            print(f"Completed {count} out of {row_count} rows, {current_time - start_time} elapsed.")
        for entry in food_names:
            if entry["fdc_id"] == f_id:
                f_pn = entry["product_name"]
                f_pd = entry["publication_date"]
                food_data.append({"source_item_id":f_id, "upc":f_upc, "name":f_pn, "category":f_cat, "db_entry_date":dt.now(), "source_item_submission_date":f_sd, "source_item_publication_date":f_pd, "serving_size":f_ss, "serving_size_unit":f_ssu})
                break
    end_time = dt.now()
    print(f"Elapsed time: {end_time - start_time}")


with open('newfile.csv', 'w') as newfile:
    fieldnames = ["fdc_id", "upc", "product_name", "serving_size", "serving_size_unit", "modified_date", "publication_date"]
    nd_w = csv.DictWriter(newfile, fieldnames=fieldnames)
    nd_w.writeheader()
    nd_w.writerows(food_data)

fieldnames = [
    "source",
    "source_item_id",
    "upc",
    "product_name",
    "product_category",
    "serving_size",
    "serving_size_unit",
    "submission_date",
    "publication_date"
]