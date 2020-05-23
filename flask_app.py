#!/usr/bin/env python3.8

from flask_pymongo import PyMongo
from flask import Flask, request, jsonify, abort
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import json
import inflect

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
#    print(f"Search query: {query}")
    best_match_rate = 0
    best_match_entry = None
    matching_entries = {}
#    print(f"Scanned product to search: {query}")
    for i in fk_products:
        for j in i:
            if 'Name' in j.keys():
                fk_match = fuzz.token_sort_ratio(str(j['Name']).lower(), query)
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
    print(f"Match rate: {best_match_rate}")
    if best_match_rate > 50:
        print(f"Best match: {best_match_name} ({best_match_rate}% confidence)")
        return [best_match_entry, best_match_rate]
    else:
#        print("Best match: None")
        return [None, None]

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
        print(f"UHTT Result: {upc_info}")
        u_name = upc_info["Name"].split()
        for i in u_name:
            if i.lower() == upc_info['BrandName'].lower():
                u_name = u_name.remove(i)
            if not i.isalpha():
                u_name = u_name.remove(i)
        upc_name = " ".join(u_name)
        basic_info = {"source": "UHTT", "result": get_storability(match_foodkeeper_product(upc_name)[0], dsr=request.args.get('s', default = 'avg', type = str)) }
        basic_info["result"]["code"] = upc_string
        basic_info["result"]["product_name"] = upc_info["Name"]
        return jsonify(basic_info), 200
    return jsonify({"source": "UHTT", "result": {"error": "Entry not found", "upc": upc_string}}), 404

@app.route('/usda/<upc_string>', methods=['GET'])
def lookup_usda(upc_string):
    s = inflect.engine()
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
        upc_data = mongo.db.usda_upc.find_one({"fdc_id": upc_name["fdc_id"]})
        print(f"UPC Name data: {upc_name}")
        print(f"UPC Product data: {upc_data}")
        upc_brand = upc_data["brand_owner"]
        upc_category = str(upc_data["branded_food_category"]) # we want to clean this value, then convert nouns to singular form before using as a foodkeeper query
        print(upc_category)
        for l in upc_category:
            if not l.isalpha(): # Prefer the first field when this value has subfields
                #prefer cookies over biscuits when given both
                if "iscuit" in upc_category and "ookie" in upc_category:
                    upc_category = "Cookie"
                if l == "&":
                    upc_category = upc_category.split("&")[0]
                if l == "/":
                    upc_category = upc_category.split("/")[0]
        print(upc_category)
        upc_cat_singular = []
        for l in upc_category.split():
            if s.singular_noun(l.strip()):
                upc_cat_singular.append(s.singular_noun(l.strip()))
            else:
                upc_cat_singular.append(l.strip())
        upc_category = " ".join(upc_cat_singular) # this is shameful

        print(f"Cleaned category value: {upc_category}")
        print(F"UPC Name: {upc_name}")
        print(get_storability(match_foodkeeper_product(f"{upc_category}")[0], dsr=request.args.get('s', default = 'avg', type = str)))
        basic_info = {"source": "USDA", "result": get_storability(match_foodkeeper_product(f"{upc_category}")[0], dsr=request.args.get('s', default = 'avg', type = str))}
        basic_info["result"]["code"] = upc_string 
        basic_info["result"]["product_name"] =  f'{upc_brand} {upc_name["description"]}'
#        print(jsonify(basic_info))
        return jsonify(basic_info), 200
#    abort(404)
    return jsonify({"source": "USDA", "result": {"error": "Entry not found", "upc": upc_string}}), 404

@app.route('/off/<upc_string>', methods=['GET'])
def lookup_off(upc_string):
    upc_orig = upc_string
    s = inflect.engine()
    if not check_input(upc_string):
        return jsonify({"error": "expected a numeric barcode."})
    while len(upc_string) < 13:
        upc_string = f"0{upc_string}"
    print(f"UPC REQUESTED FROM OPENFOODFACTS: {str(upc_string)}")
    product_info = mongo.db.openfoodfacts.find_one({"code": upc_string})
    print(product_info)
    if product_info:
        c_stor = None
        if "categories_hierarchy" in product_info.keys():
            c_stor = None
            c_s = [None, 0]
            c_h = product_info["categories_hierarchy"][::-1]
            for i in range(len(c_h)):
                c_h[i] = s.singular_noun(c_h[i].split(":")[1])
            # prefer cookies over biscuits in snack categories
            if "sweet-snack" in c_h and "biscuit" == c_h[0]:
                c_h[0] = "cookie"
            print(f"Selected Category: {c_h[0]}")
            c_r = match_foodkeeper_product(c_h[0])
            c_stor = get_storability(c_r[0], dsr=request.args.get('s', default = 'avg', type = str))
            # for i in range(len(c_h)):
                # c_q = " ".join(c_h[0:i])
                # c_r = match_foodkeeper_product(c_q)
                # if c_r[1] and c_r[1] > c_s[1]:
#                    print(c_r, c_s)
                    # c_s = c_r
#                    print(c_r, c_s)
                    # c_stor = get_storability(c_r[0], dsr=request.args.get('s', default = 'avg', type = str))
#                    print(f"c_stor is {c_stor}")
            print(f"Product categories: {product_info['categories_hierarchy']}")
        else:
            c_stor = get_storability(match_foodkeeper_product(f'{s.singular_noun(product_info["product_name"])}')[0], dsr=request.args.get('s', default = 'avg', type = str))
#        print(f"c_stor is {c_stor}")
        basic_info = {"source": "OpenFoodFacts", "result": c_stor } # refactor this and catch errors when getting hierarchy
        print(basic_info)
        basic_info["result"]["code"] = str(upc_orig)
        basic_info["result"]["product_name"] = product_info["product_name"]
#    return mongo.db.product.PyMongo.find_one({"code": upc_string})
        return jsonify(basic_info), 200
    return jsonify({"source": "OpenFoodFacts", "result": {"error": "Entry not found", "upc": upc_orig}}), 404

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
            # p_name = j["result"]["product_name"]
            # result = get_storability(match_foodkeeper_product(p_name))
            # result["product_name"] = p_name
            #result["upc"] = upc_string
            return jsonify(j["result"])
    if not found:
        result = {"error": "Entry not found", "upc": upc_string}
        return jsonify(result)



#get_storability(match_foodkeeper_product("Yoplait Original Harvest Peach Low Fat Yogurt")[0])
#get_storability(552.0)
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
