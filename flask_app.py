#!/usr/bin/env python3.8

from flask_pymongo import PyMongo
from flask import Flask, request, jsonify, abort
app = Flask("__name__")
#app.config["MONGO_DBNAME"] = "test"
app.config['JSON_SORT_KEYS'] = False #Because ordered data is pretty data too
app.config["MONGO_URI"] = "mongodb://127.0.0.1:27017/upc-data"
mongo = PyMongo(app)

@app.route('/uhtt/<upc_string>', methods=['GET'])
def lookup_uhtt(upc_string):
    print(f"UPC REQUESTED FROM UHTT: {upc_string}")
    upc_info = mongo.db.uhtt.find_one({"UPCEAN": int(upc_string)})
    if upc_info:
        basic_info = {"source": "UHTT", "result": {"code": upc_string, "product_name": upc_info["Name"]}}
        return jsonify(basic_info), 200
    return jsonify({"source": "UHTT", "result": {"error": "Entry not found", "upc": upc_string}}), 404

@app.route('/usda/<upc_string>', methods=['GET'])
def lookup_usda(upc_string):
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
        basic_info = {"source": "USDA", "result": {"code": upc_string, "product_name": f'{upc_brand} {upc_name["description"]}'}}
        print(jsonify(basic_info))
        return jsonify(basic_info), 200
#    abort(404)
    return jsonify({"source": "USDA", "result": {"error": "Entry not found", "upc": upc_string}}), 404

@app.route('/off/<upc_string>', methods=['GET'])
def lookup_off(upc_string):
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
#    results = {"results": [lookup_off(upc_string).get_json(), lookup_usda(upc_string).get_json(), lookup_uhtt(upc_string).get_json()]}
    print(lookup_off(upc_string))
    return "doot"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port="5555")
