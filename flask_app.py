#!/usr/bin/env python3.8

from flask_pymongo import PyMongo
from flask import Flask, request, jsonify, abort
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import json

DEFAULT_STORABILITY_RANGE = "min" # Choose from "min", "max", or "avg"

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
    print(f"Scanned product to search: {query}")
    for i in fk_products:
        for j in i:
            if 'Keywords' in j.keys():
                fk_match = fuzz.partial_ratio(str(j['Keywords']).lower(), query)
                if fk_match > 50:
                    if i[3]['Name_subtitle'] == None:
                        match_name = i[2]['Name']
                    else:
                        match_name = f"{i[2]['Name']} ({i[3]['Name_subtitle']})"
                    matching_entries[match_name] = fk_match
                if fk_match > best_match_rate:
                    best_match_rate = fk_match
                    best_match_entry = i[0]["ID"]
    print(f"Search matches: {matching_entries}")
#    print(f"Match rate: {best_match_rate}")
#    print(best_match_entry)
    if best_match_rate > 50:
        return best_match_entry
    else:
        return None

def get_storability(id, dsr=DEFAULT_STORABILITY_RANGE):
    p_stor = []
    po_stor = []
    r_stor = []
    ro_stor = []
    for i in fk_products:
        if i[0]["ID"] == id:
            for j in i:
                for k in j.keys():
                    for l in ["Pantry_Min", "Pantry_Max", "Pantry_Metric"]:
                        if l in k and None not in j.values():
                            p_stor.append(j)
                    for l in ["Pantry_After_Opening_Min", "Pantry_After_Opening_Max", "Pantry_After_Opening_Metric"]:
                        if l in k and None not in j.values():
                            po_stor.append(j)
                    for l in ["Refrigerate_Min", "Refrigerate_Max", "Refrigerate_Metric"]:
                        if l in k and None not in j.values():
                            r_stor.append(j)
                    for l in ["Refrigerate_After_Opening_Min", "Refrigerate_After_Opening_Max", "Refrigerate_After_Opening_Metric"]:
                        if l in k and None not in j.values():
                            ro_stor.append(j)
    for i in [p_stor, po_stor, r_stor, ro_stor]:
        if len(i) == 3:
            min = list(i[0].values())[0]
            if int(min) == min:
                min = int(min)
            max = list(i[1].values())[0]
            if int(max) == max:
                max = int(max)
            avg = (min+max)/2
            if int(avg) == avg:
                avg = int(avg)
            metric = list(i[2].values())[0]
            if "o_" in i:
                print("After opening:")
            elif "p" in i:
                print("Pantry storage:")
            else:
                print("Refrigerated storage:")
            if dsr == "min":
                print(f"{min} {metric}")
            if dsr == "max":
                print(f"{max} {metric}")
            if dsr == "avg":
                print(f"{avg} {metric}")

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

#get_storability(match_foodkeeper("Yoplait Original Harvest Peach Low Fat Yogurt"))

for i in ["min", "max", "avg"]:
    get_storability(match_foodkeeper("Yoplait Original Harvest Peach Low Fat Yogurt"), i)

# for i in fk_products:
#     if i[3]['Name_subtitle'] == None:
#         match_name = i[2]['Name']
#     else:
#         match_name = f"{i[2]['Name']} ({i[3]['Name_subtitle']})"
#     print(match_name)
#     get_storability(i[0]["ID"])

#if __name__ == "__main__":
#    app.run(host="0.0.0.0", port="5555")
