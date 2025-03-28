from flask import (request, jsonify, Blueprint)
from .data_store import products

products_blueprint = Blueprint('products_api', __name__, url_prefix='/api/')


@products_blueprint.route('/products', methods=['GET'])
def list_products():
    return jsonify(products)

@products_blueprint.route('/products/<product_id>', methods=['GET'])
def get_product(product_id):
    result = next((product for product in products if product['id'] == product_id), None)
    if result is None:
        return jsonify({'error': 'Product not found'})
    return jsonify(result)
