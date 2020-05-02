from flask_pymongo import PyMongo
from flask import Flask, request, jsonify
app = Flask("__name__")
#app.config["MONGO_DBNAME"] = "test"
app.config["MONGO_URI"] = "mongodb://127.0.0.1:27017/test"
mongo = PyMongo(app)
@app.route('/lookup/<upc_string>', methods=['GET'])
def lookup_by_upc(upc_string):
    upc_string = str(upc_string)
    print(f"UPC REQUESTED: {upc_string}")
    product_info = mongo.db.products.find_one_or_404({"code": upc_string})
    print(type(product_info))
    basic_info = {"code": product_info["code"], "product_name": product_info["product_name"]}
#    return mongo.db.product.PyMongo.find_one({"code": upc_string})
    return jsonify(basic_info)
if __name__ == "__main__":
    app.run(host="0.0.0.0", port="5555")
