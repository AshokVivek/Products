from flask import Flask

from .api.products_api import products_blueprint


def create_app():
    application = Flask(__name__)
    application.register_blueprint(products_blueprint)
    return application


app = create_app()