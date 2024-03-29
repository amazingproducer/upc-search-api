#!/usr/bin/env python3.8

from flask_pymongo import PyMongo
from flask import Flask, request, jsonify, abort
app = Flask("__name__")
app.config['JSON_SORT_KEYS'] = False #Because ordered data is pretty data too
app.config["MONGO_URI"] = "mongodb://127.0.0.1:27017/upc-data"
mongo = PyMongo(app)

def check_input(upc_string):
    if upc_string.isnumeric():
        return True
    return False

@app.route('/uhtt/<upc_string>', methods=['GET'])
def lookup_uhtt(upc_string):
    if not check_input(upc_string):
        return jsonify({"error": "expected a numeric barcode"}), 404 # 404 is probably not the right code for this error type.
    print(f"UPC REQUESTED FROM UHTT: {upc_string}")
    upc_info = mongo.db.uhtt.find_one({"UPCEAN": int(upc_string)})
    if upc_info:
        basic_info = {"source": "UHTT", "result": {"upc": upc_string, "product_name": upc_info["Name"]}}
        return jsonify(basic_info), 200
    return jsonify({"source": "UHTT", "result": { "upc": upc_string, "error": "entry not found"}}), 404

@app.route('/usda/<upc_string>', methods=['GET'])
def lookup_usda(upc_string):
    if not check_input(upc_string):
        return jsonify({"error": "expected a numeric barcode"}), 404
    print(f"UPC REQUESTED FROM USDA: {upc_string}")
#    upc_info = mongo.db.usda_upc.find({"gtin_upc": int(upc_string)}).sort([("available_date",-1)])[0]
    upc_results = mongo.db.usda_upc.find({"gtin_upc": int(upc_string)})
    fdc_ids = []
    for i in upc_results:
        fdc_ids.append(i["fdc_id"])
    if len(fdc_ids) > 0:
        upc_name = mongo.db.usda_name.find({"fdc_id": {"$in": fdc_ids}}).sort([("publication_date", -1)])[0]
#        print(f'Found latest FDC entry: {upc_name["fdc_id"]}')
        upc_brand = mongo.db.usda_upc.find_one({"fdc_id": upc_name["fdc_id"]})["brand_owner"]
        basic_info = {"source": "USDA", "result": {"upc": upc_string, "product_name": f'{upc_brand} {upc_name["description"]}'}}
#        print(jsonify(basic_info))
        return jsonify(basic_info), 200
#    abort(404)
    return jsonify({"source": "USDA", "result": {"upc": upc_string, "error": "entry not found"}}), 404

@app.route('/off/<upc_string>', methods=['GET'])
def lookup_off(upc_string):
    if not check_input(upc_string):
        return jsonify({"error": "expected a numeric barcode"}), 404
    while len(upc_string) < 13:
        upc_string = f"0{upc_string}"
    print(f"UPC REQUESTED FROM OPENFOODFACTS: {str(upc_string)}")
    product_info = mongo.db.openfoodfacts.find_one({"code": upc_string})
#    print(type(product_info))
    if product_info:
        basic_info = {"source": "OpenFoodFacts", "result": {"upc": upc_string, "product_name": product_info["product_name"]}}
#    return mongo.db.product.PyMongo.find_one({"code": upc_string})
        return jsonify(basic_info), 200
    return jsonify({"source": "OpenFoodFacts", "result": {"upc": upc_string, "error": "entry not found"}}), 404


# oof
@app.route('/lookup/<upc_string>', methods=['GET'])
def lookup(upc_string):
    if not check_input(upc_string):
        return jsonify({"error": "expected a numeric barcode"}), 404
    results = {"results": [lookup_off(upc_string)[0].get_json(), lookup_usda(upc_string)[0].get_json(), lookup_uhtt(upc_string)[0].get_json()]}
#    print(lookup_off(upc_string)[0].get_json())
#    print(lookup_usda(upc_string))
#    print(lookup_uhtt(upc_string))
    return jsonify(results)

@app.route('/grocy/<upc_string>', methods=['GET'])
def grocy_barcode_name_search(upc_string):
    if not check_input(upc_string):
        return jsonify({"error": "expected a numeric barcode"})
    found = None
    sources = [lookup_off, lookup_usda, lookup_uhtt]
    for source in sources:
        j = source(upc_string)[0].get_json()
        if ("error" not in j["result"]) and (not found):
            found = True
            result = {"upc": upc_string, "product_name": j["result"]["product_name"]}
            return jsonify(result), 200
    if not found:
        result = {"upc": upc_string, "error": "entry not found"}
        return jsonify(result), 404



if __name__ == "__main__":
    app.run(host="0.0.0.0", port="5555")
