import os
import json
import redis
import threading
import pickle #nosec
import time
from library.utils import get_compiled_regex_list
from redis.backoff import ExponentialBackoff
from redis.retry import Retry
from redis.exceptions import (ConnectionError, TimeoutError)
from python.api_utils import call_api_with_session

IS_SERVER = os.environ.get('IS_SERVER', False) in ["true", "1", "t"]

DJANGO_BASE_URL = os.environ.get('DJANGO_BASE_URL')
INTERNAL_API_KEY = os.environ.get('INTERNAL_API_KEY')

BANK_CONNECT_APIS_REDIS_HOST = os.environ.get('BANK_CONNECT_APIS_REDIS_HOST')
BANK_CONNECT_APIS_REDIS_PORT = os.environ.get('BANK_CONNECT_APIS_REDIS_PORT')
BANK_CONNECT_APIS_REDIS_SSL_ENABLED = os.environ.get('BANK_CONNECT_APIS_REDIS_SSL_ENABLED', False) in ["true", "1", "t"]

redis_protocol = "rediss" if BANK_CONNECT_APIS_REDIS_SSL_ENABLED else "redis"
redis_url = f"{redis_protocol}://{BANK_CONNECT_APIS_REDIS_HOST}:{BANK_CONNECT_APIS_REDIS_PORT}/0"

HEADERS = {
    'x-api-key': INTERNAL_API_KEY
}

class RedisClient:
    _instance = None
    _lock = threading.Lock()  # Ensures thread safety for first initialization
    _retry = Retry(backoff=ExponentialBackoff(), retries=5)
    _retry_modes = [ConnectionError, TimeoutError]
    
    def __new__(cls, redis_url):
        if not cls._instance:
            with cls._lock:  # Thread-safe initialization
                if not cls._instance:  # Double-check locking
                    print("RedisConnection: initializing redis connection pool")
                    cls._instance = super(RedisClient, cls).__new__(cls)
                    cls._instance.pool = redis.ConnectionPool.from_url(redis_url)
                    cls._instance._conn = redis.Redis(
                        connection_pool=cls._instance.pool, 
                        retry=cls._retry, 
                        retry_on_error=cls._retry_modes
                    )
        return cls._instance

    @property
    def conn(self):
        """Always use the pooled Redis connection"""
        try:
            if self._conn.ping():
                return self._conn
        except redis.ConnectionError:
            print("RedisConnection: reconnecting due to failure")
            self._conn = redis.Redis(
                connection_pool=self.pool,
                retry=self._retry,
                retry_on_error=self._retry_modes
            )  # Reconnect using the pool
        return self._conn

if IS_SERVER:
    redis_client = RedisClient(redis_url).conn

def get_everything_from_server(bank_name, country="IN"):
    url = DJANGO_BASE_URL + f"/bank-connect/v1/internal/get_enrichment_regexes/?type=all&bank_name={bank_name}&country={country}"
    response = call_api_with_session(url, "GET", None, HEADERS).json()
    time.sleep(2) # this sleep ensures the replica coming in sync with the master node, since this call happens rarely
    return response

#setting bank_name as SBI in case bank_name is not required for few keys like lender_list, 
#but django's cache loading api required bank_name, so passing SBI as default 
def get_dict_from_redis(key,  country='IN', bank_name='SBI'):
    data = redis_client.get(key)

    if not data:
        print("RedisConnection: cache not found calling django apis for reloading")
        get_everything_from_server(bank_name, country)
        data = redis_client.get(key)

    if not data:
        raise Exception(f"Regex data not found in redis for key {key}")
    
    pickle_data = pickle.loads(data) #nosec
    json_data = json.loads(pickle_data)
    return json_data


def get_transaction_templates(bank_name):
    redis_path = f":1:extraction_template_{bank_name}"
    data = get_dict_from_redis(redis_path, bank_name=bank_name)
    
    trans_bbox = data["transactions_template"]
    last_page_regex = data["last_page_regex_templates"]
    account_delimiter_regex = data["account_delimiter_regex"]
    return trans_bbox, last_page_regex, account_delimiter_regex

def get_cc_transaction_templates(bank_name):
    redis_path = f":1:cc_templates_{bank_name}"
    data = get_dict_from_redis(redis_path, bank_name)
    return data

def get_description_regexes(country="IN"):
    # converting country to uppercase as a safety check
    country = country.upper()

    if IS_SERVER:
        tmp_redis_path = f":1:description_{country}"
        data = get_dict_from_redis(tmp_redis_path, country)
        return data

    # first check if description for this country is cached and kept in /tmp
    tmp_file_path = f"/tmp/description_{country}.json"

    data = {}
    
    if os.path.exists(tmp_file_path):
        data = json.load(open(tmp_file_path))
        return data
    
    return {}

def get_unclean_merchant(bank_name, country="IN"):
    if bank_name in ["federal1", "india_post1"]:
        bank_name = bank_name[:-1]

    # converting country to uppercase as a safety check
    country = country.upper()

    if IS_SERVER:
        tmp_redis_path = f":1:unclean_merchant_{bank_name}_{country}"
        final_data = get_dict_from_redis(tmp_redis_path, country, bank_name)

        final_data["merchant_debit_regex_list"] = final_data.pop('debit')
        final_data["merchant_credit_regex_list"] = final_data.pop('credit')

        final_data["merchant_debit_regex_list"] = get_compiled_regex_list(final_data["merchant_debit_regex_list"])
        final_data["merchant_credit_regex_list"] = get_compiled_regex_list(final_data["merchant_credit_regex_list"])
        return final_data

    # check if the data is present in the /tmp folder 
    tmp_file_path = f"/tmp/unclean_merchant_{bank_name}_{country}.json"
    
    final_data = {}
    if os.path.exists(tmp_file_path):
        final_data = json.load(open(tmp_file_path))
        # get compiled regexes for the files
        final_data["merchant_debit_regex_list"] = get_compiled_regex_list(final_data["merchant_debit_regex_list"])
        final_data["merchant_credit_regex_list"] = get_compiled_regex_list(final_data["merchant_credit_regex_list"])
    return final_data

def get_transaction_channel_lists(bank_name, country="IN"):
    if bank_name in ["federal1", "india_post1"]:
        bank_name = bank_name[:-1]

    if IS_SERVER:
        tmp_redis_path = f":1:transaction_channel_{bank_name}_{country}"
        final_data = get_dict_from_redis(tmp_redis_path, country, bank_name)
        
        final_data['debit_channel_dict'] = final_data.pop('debit', [])
        final_data['credit_channel_dict'] =  final_data.pop('credit', [])
        for key in final_data["debit_channel_dict"]:
            final_data["debit_channel_dict"][key] = get_compiled_regex_list(final_data["debit_channel_dict"][key])

        for key in final_data["credit_channel_dict"]:
            final_data["credit_channel_dict"][key] = get_compiled_regex_list(final_data["credit_channel_dict"][key])
        final_data["debit_priority_order"] = list(final_data["debit_channel_dict"].keys())
        final_data["credit_priority_order"] = list(final_data["credit_channel_dict"].keys())
        return final_data
        
    # check if the data is present in the /tmp folder 
    tmp_file_path = f"/tmp/transaction_channel_{bank_name}_{country}.json"

    final_data = {
        "debit_channel_dict": [],
        "credit_channel_dict": [],
    }
    if os.path.exists(tmp_file_path):
        final_data = json.load(open(tmp_file_path))
        # get compiled regexes for the files
        for key in final_data["debit_channel_dict"]:
            final_data["debit_channel_dict"][key] = get_compiled_regex_list(final_data["debit_channel_dict"][key])

        for key in final_data["credit_channel_dict"]:
            final_data["credit_channel_dict"][key] = get_compiled_regex_list(final_data["credit_channel_dict"][key])

    return final_data

def get_lender_list(country="IN"):
    if IS_SERVER:
        tmp_redis_path = f":1:lender_list_{country}"
        return get_dict_from_redis(tmp_redis_path, country).get('lenders', [])

    # first check if the lender_list for this country exists in the /tmp folder
    tmp_file_path = f"/tmp/lender_list_{country}.json"
    if os.path.exists(tmp_file_path):
        data = json.load(open(tmp_file_path))
        return data
    
    return []

def get_lender_regex_list(country: str = "IN") -> list:
    if IS_SERVER:
        tmp_redis_path = f":1:lender_regex_list_{country}"
        return get_dict_from_redis(tmp_redis_path, country)

    # first check if the lender_regex_list for this country exists in the /tmp folder
    tmp_file_path = f"/tmp/lender_regex_list_{country}.json"
    if os.path.exists(tmp_file_path):
        data = json.load(open(tmp_file_path))
        return data
    
    return None

def get_merchant_category_data(country="IN"):
    if IS_SERVER:
        tmp_redis_path = f":1:merchant_category_{country}"
        return get_dict_from_redis(tmp_redis_path, country)

    # first check if merchant category for this country is cached and kept in /tmp
    tmp_file_path = f"/tmp/merchant_category_{country}.json"
    if os.path.exists(tmp_file_path):
        data = json.load(open(tmp_file_path))
        return data
    
    return {}