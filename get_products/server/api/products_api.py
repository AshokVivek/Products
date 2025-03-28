from flask import (request, jsonify, Blueprint)
from .data_store import products

products_blueprint = Blueprint('products_api', __name__, url_prefix='/api/')


@products_blueprint.route('/products', methods=['GET'])
def list_products():
    return jsonify(products)