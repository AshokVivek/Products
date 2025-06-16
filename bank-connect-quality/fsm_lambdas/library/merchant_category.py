from library.enrichment_regexes import get_merchant_category_data

def get_merchant_category_dict(country="IN"):
    return get_merchant_category_data(country)