#!/usr/bin/env python3.8

from flask_pymongo import PyMongo
from flask import Flask, request, jsonify, abort
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import json

DEFAULT_STORABILITY_RANGE = "min" # Choose from "min", "max", or "avg"
USE_PRODUCT_USERFIELDS = True # Setting to True requires grocy product userfields:
# refrigerate_after_opening: Boolean
# refrigeration_required: Boolean

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


def match_foodkeeper_category(query):
    # Do some fuzzy searching through foodkeeper's categories and products
    # in order to find a match for our product query.
    best_match_rate = 0
    best_match_entry = None
    matching_entries = {}
    print(f"Scanned product to search: {query}")
    for i in fk_categories:
        fk_match = fuzz.token_set_ratio(str(f"{i[1]['Category_Name']} {i[2]['Subcategory_Name']}").lower(), query)
        if fk_match > 50:
            if i[2]['Subcategory_Name'] == None:
                match_name = i[1]['Category_Name']
            else:
                match_name = f"{i[1]['Category_Name']} ({i[2]['Subcategory_Name']})"
            matching_entries[match_name] = fk_match
        if fk_match > best_match_rate:
            best_match_rate = fk_match
            best_match_entry = i[0]["ID"]
            if i[2]['Subcategory_Name'] == None:
                best_match_name = i[1]['Category_Name']
            else:
                best_match_name = f"{i[1]['Category_Name']} ({i[2]['Subcategory_Name']})"
    if len(matching_entries):
        print(f"Search matches: {matching_entries}")
    else:
        print("Search matches: None")
#    print(f"Match rate: {best_match_rate}")
    if best_match_rate > 50:
        print(f"Best match: {best_match_name} ({best_match_rate}% confidence)")
        return best_match_entry
    else:
        print("Best match: None")
        return None


def match_foodkeeper_product(query):
    # Do some fuzzy searching through foodkeeper's categories and products
    # in order to find a match for our product query.
    best_match_rate = 0
    best_match_entry = None
    matching_entries = {}
    print(f"Scanned product to search: {query}")
    for i in fk_products:
        for j in i:
            if 'Keywords' in j.keys():
                fk_match = fuzz.ratio(str(j['Keywords']).lower(), query)
                if fk_match > 50:
                    if i[3]['Name_subtitle'] == None:
                        match_name = i[2]['Name']
                    else:
                        match_name = f"{i[2]['Name']} ({i[3]['Name_subtitle']})"
                    matching_entries[match_name] = fk_match
                if fk_match > best_match_rate:
                    best_match_rate = fk_match
                    best_match_entry = i[0]["ID"]
                    if i[3]['Name_subtitle'] == None:
                        best_match_name = i[2]['Name']
                    else:
                        best_match_name = f"{i[2]['Name']} ({i[3]['Name_subtitle']})"
    if len(matching_entries):
        print(f"Search matches: {matching_entries}")
#    print(f"Match rate: {best_match_rate}")
    if best_match_rate > 50:
        print(f"Best match: {best_match_name} ({best_match_rate}% confidence)")
        return best_match_entry
    else:
        print("Best match: None")
        return None

def get_storability(id, dsr=DEFAULT_STORABILITY_RANGE):
    # TODO get category for product when possible to refine this
    p_stor = []
    po_stor = []
    r_stor = []
    ro_stor = []
    storability = {}
    userfields = {}
    m_ratio = {"Days":1, "Weeks":7, "Months":30, "Years":365}
    for i in fk_products:
        if i[0]["ID"] == id:
#            print(i)
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
            s_key = None
            min_i = list(i[0].values())[0]
            if int(min_i) == min_i:
                min_i = int(min_i)
            max_i = list(i[1].values())[0]
            if int(max_i) == max_i:
                max_i = int(max_i)
            avg_i = (min_i+max_i)/2
            if int(avg_i) == avg_i:
                avg_i = int(avg_i)
            metric = list(i[2].values())[0]
            if "Pantry_After" in list(i[0].keys())[0]:
#                print("After opening:")
                s_key = 'default_best_before_days_after_open'
            elif "Refrigerate_After" in list(i[0].keys())[0]:
#                print("Refrigerate after opening!")
                s_key = 'default_best_before_days_after_open'
                if USE_PRODUCT_USERFIELDS:
                    userfields["refrigerate_after_opening"] = True
                    storability['userfields'] = userfields
            elif "Pantry_M" in list(i[0].keys())[0]:
#                print("Pantry storage:")
                s_key = 'default_best_before_days'
            elif "Refrigerate_M" in list(i[0].keys())[0]:
#                print("Refrigerated storage required!")
                s_key = 'default_best_before_days'
                if USE_PRODUCT_USERFIELDS:
                    userfields['refrigeration_required'] = True
                    storability['userfields'] = userfields
            if metric in m_ratio.keys():
                if dsr == "min":
                    storability[s_key] = min_i * m_ratio[metric]
#                    print(f"{min_i} {metric}")
                if dsr == "max":
                    storability[s_key] = max_i * m_ratio[metric]
#                    print(f"{max_i} {metric}")
                if dsr == "avg":
                    storability[s_key] = avg_i * m_ratio[metric]
#                    print(f"{avg_i} {metric}")
            else:
                return {"Error": "Unsupported storability metric."}
    print(storability)
    return(storability)

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
    if not check_input(upc_string):
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
        print(product_info)
        basic_info = {"source": "OpenFoodFacts", "result": get_storability(match_foodkeeper_product(f'{product_info["product_name"]} {" ".join(product_info["_keywords"])}'), dsr=request.args.get('s', default = 'avg', type = str)) }
        print(basic_info)
        basic_info["result"]["code"] = str(upc_string)
        basic_info["result"]["product_name"] = product_info["product_name"]
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
            p_name = j["result"]["product_name"]
            result = get_storability(match_foodkeeper_product(p_name))
            result["product_name"] = p_name
            result["upc"] = upc_string
            return jsonify(result)
    if not found:
        result = {"error": "Entry not found", "upc": upc_string}
        return jsonify(result)



#get_storability(match_foodkeeper_product("Yoplait Original Harvest Peach Low Fat Yogurt"))

#get_storability(match_foodkeeper_product("Best Foods Mayonnaise, 32 oz."))

#grocy_barcode_name_search("070470290614")

#match_foodkeeper_category("Best Foods Mayonnaise, 32 oz.")

# for i in fk_products:
#     if i[3]['Name_subtitle'] == None:
#         match_name = i[2]['Name']
#     else:
#         match_name = f"{i[2]['Name']} ({i[3]['Name_subtitle']})"
#     print(match_name)
#     get_storability(i[0]["ID"])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port="5555")
