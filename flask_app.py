#!/usr/bin/env python3.8

from flask_pymongo import PyMongo
from flask import Flask, request, jsonify, abort
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import json

app = Flask("__name__")
app.config['JSON_SORT_KEYS'] = False #Because ordered data is pretty data too
app.config["MONGO_URI"] = "mongodb://127.0.0.1:27017/upc-data"
mongo = PyMongo(app)

fk_file = open('./foodkeeper.json', 'r')
fk = json.load(fk_file)
fk_categories = fk['sheets'][1]['data']
fk_products = fk['sheets'][2]['data']
fk_names = []
fk_keywords = []

for i in fk_products:
    for j in i:
        if 'Name' in j.keys():
            fk_names.append(j['Name']) # this makes a list of product names, but some names are identical
        if 'Keywords' in j.keys():
            fk_keywords.append(j['Keywords']) # this makes a list of keyword sets, but some keyword sets are strikingly similar


def check_input(upc_string):
    if upc_string.isnumeric():
        return True
    return False

def match_foodkeeper(query):
    # Do some fuzzy searching through foodkeeper's categories and products
    # in order to find a match for our product query.
    best_match_rate = 0
    best_match_entry = None
    matching_entries = {}
    for i in fk_products:
        for j in i:
            if 'Keywords' in j.keys():
                fk_match = fuzz.partial_ratio(str(j['Keywords']).lower(), query)
                if fk_match > 85:
                    matching_entries[f"{i[0]['ID']}"] = fk_match
                if fk_match > best_match_rate:
                    best_match_rate = fk_match
                    best_match_entry = i
    print(matching_entries)
    print(f"Match rate: {best_match_rate}")
    print(best_match_entry)

@app.route('/uhtt/<upc_string>', methods=['GET'])
def lookup_uhtt(upc_string):
    if not check_input(upc_string):
        return jsonify({"error": "expected a numeric barcode."})
    print(f"UPC REQUESTED FROM UHTT: {upc_string}")
    upc_info = mongo.db.uhtt.find_one({"UPCEAN": int(upc_string)})
    if upc_info:
        basic_info = {"source": "UHTT", "result": {"code": upc_string, "product_name": upc_info["Name"]}}
        return jsonify(basic_info), 200
    return jsonify({"source": "UHTT", "result": {"error": "Entry not found", "upc": upc_string}}), 404

@app.route('/usda/<upc_string>', methods=['GET'])
def lookup_usda(upc_string):
    if not check_input(upc_string, more):
        return jsonify({"error": "expected a numeric barcode."})
    print(f"UPC REQUESTED FROM USDA: {upc_string}")
#    upc_info = mongo.db.usda_upc.find({"gtin_upc": int(upc_string)}).sort([("available_date",-1)])[0]
    upc_results = mongo.db.usda_upc.find({"gtin_upc": int(upc_string)})
    fdc_ids = []
    for i in upc_results:
        fdc_ids.append(i["fdc_id"])
    if len(fdc_ids) > 0:
        upc_name = mongo.db.usda_name.find({"fdc_id": {"$in": fdc_ids}}).sort([("publication_date", -1)])[0]
#        print(f'Found latest FDC entry: {upc_name["fdc_id"]}')

# TODO this should fill the 'more' request, but the cart is before the horse at this point
        # if more:
        #     u = mongo.db.usda_upc.find_one({"fdc_id": upc_name["fdc_id"]})
        #     m = {}
        #     m["product_name"] = f'{u["brand_owner"]} {upc_name["description"]}'
        #     m["default_best_before_days"] = "" # TODO fill this out ayyyy
        #     m["default_best_before_days_after_open"]




        upc_brand = mongo.db.usda_upc.find_one({"fdc_id": upc_name["fdc_id"]})["brand_owner"]
        basic_info = {"source": "USDA", "result": {"code": upc_string, "product_name": f'{upc_brand} {upc_name["description"]}'}}
#        print(jsonify(basic_info))
        return jsonify(basic_info), 200
#    abort(404)
    return jsonify({"source": "USDA", "result": {"error": "Entry not found", "upc": upc_string}}), 404

@app.route('/off/<upc_string>', methods=['GET'])
def lookup_off(upc_string):
    if not check_input(upc_string):
        return jsonify({"error": "expected a numeric barcode."})
    while len(upc_string) < 13:
        upc_string = f"0{upc_string}"
    print(f"UPC REQUESTED FROM OPENFOODFACTS: {str(upc_string)}")
    product_info = mongo.db.openfoodfacts.find_one({"code": upc_string})
#    print(type(product_info))
    if product_info:
        basic_info = {"source": "OpenFoodFacts", "result": {"code": upc_string, "product_name": product_info["product_name"]}}
#    return mongo.db.product.PyMongo.find_one({"code": upc_string})
        return jsonify(basic_info), 200
    return jsonify({"source": "OpenFoodFacts", "result": {"error": "Entry not found", "upc": upc_string}}), 404

@app.route('/lookup/<upc_string>', methods=['GET'])
def lookup(upc_string):
    if not check_input(upc_string):
        return jsonify({"error": "expected a numeric barcode."})
    results = {"results": [lookup_off(upc_string)[0].get_json(), lookup_usda(upc_string)[0].get_json(), lookup_uhtt(upc_string)[0].get_json()]}
#    print(lookup_off(upc_string)[0].get_json())
#    print(lookup_usda(upc_string))
#    print(lookup_uhtt(upc_string))
    return jsonify(results)

@app.route('/grocy/<upc_string>', methods=['GET'])
def grocy_barcode_name_search(upc_string):
    if not check_input(upc_string):
        return jsonify({"error": "expected a numeric barcode."})
    found = None
    sources = [lookup_off, lookup_usda, lookup_uhtt]
    for source in sources:
        j = source(upc_string)[0].get_json()
        if ("error" not in j["result"]) and (not found):
            found = True
            result = {"product_name": j["result"]["product_name"], "upc": upc_string}
            return jsonify(result)
    if not found:
        result = {"error": "Entry not found", "upc": upc_string}
        return jsonify(result)

match_foodkeeper("hummus")

#if __name__ == "__main__":
#    app.run(host="0.0.0.0", port="5555")
