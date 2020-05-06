#!/usr/bin/env python3.8

from flask_pymongo import PyMongo
from flask import Flask, request, jsonify
app = Flask("__name__")
#app.config["MONGO_DBNAME"] = "test"
app.config["MONGO_URI"] = "mongodb://127.0.0.1:27017/upc-data"
mongo = PyMongo(app)

@app.route('/uhtt/<upc_string>', methods=['GET'])
def lookup_uhtt(upc_string):
    print(f"UPC REQUESTED FROM UHTT: {upc_string}")
    upc_info = mongo.db.uhtt.find_one_or_404({"UPCEAN": int(upc_string)})
    basic_info = {"code": upc_info["UPCEAN"], "product_name": upc_info["Name"]}
    return jsonify(basic_info)

@app.route('/usda/<upc_string>', methods=['GET'])
def lookup_usda(upc_string):
    print(f"UPC REQUESTED FROM USDA: {upc_string}")
    upc_info = mongo.db.usda_upc.find({"gtin_upc": int(upc_string)}).sort({"available_date":-1})
    print(upc_info)
    upc_name = mongo.db.usda_name.find_one_or_404({"fdc_id": upc_info["fdc_id"]})
#    print(type(product_info))
    basic_info = {"code": upc_info["gtin_upc"], "product_name": upc_name["description"]}
    return jsonify(basic_info)

@app.route('/off/<upc_string>', methods=['GET'])
def lookup_off(upc_string):
    print(f"UPC REQUESTED FROM OPENFOODFACTS: {str(upc_string)}")
    product_info = mongo.db.openfoodfacts.find_one_or_404({"code": upc_string})
#    print(type(product_info))
    basic_info = {"code": product_info["code"], "product_name": product_info["product_name"]}
#    return mongo.db.product.PyMongo.find_one({"code": upc_string})
    return jsonify(basic_info)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port="5555")
