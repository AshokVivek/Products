import requests
import json
from app.conf import BANK_CONNECT_BASE_URL, API_KEY, SERVER_HASH, SUPERUSER_TOKEN

def fb_dashboard_api_create_or_update(body: dict):
    url =  f'{BANK_CONNECT_BASE_URL}/bank-connect/v1/internal/create_or_update_template/'
    headers = {
        "x-api-key": API_KEY,
        "server-hash": SERVER_HASH,
        "Content-Type": "application/json",
        "Authorization": SUPERUSER_TOKEN
    }
    body = json.dumps(body)
    print("request body --> ", body)
    print("url --> ", url)
    response = requests.request("POST", url, headers=headers, data=body)
    print(f"response : status -> {response.status_code}, json -> {response.json()}")

    return response

def fb_dashboard_api_create_or_update_category_regex(body: dict):
    url =  f'{BANK_CONNECT_BASE_URL}/bank-connect/v1/internal/create_or_update_category_regex/'
    headers = {
        "x-api-key": API_KEY,
        "server-hash": SERVER_HASH,
        "Content-Type": "application/json",
        "Authorization": SUPERUSER_TOKEN
    }
    body = json.dumps(body)
    print("request body --> ", body)
    print("url --> ", url)
    response = requests.request("POST", url, headers=headers, data=body)
    print(f"response : status -> {response.status_code}, json -> {response.json()}")

    return response


def fb_dashboard_api_cc_create_or_update(body: dict):
    url = f'{BANK_CONNECT_BASE_URL}/bank-connect/v1/internal/create_or_update_cc_template/'
    headers = {
        "x-api-key": API_KEY,
        "server-hash": SERVER_HASH,
        "Content-Type": "application/json",
        "Authorization": SUPERUSER_TOKEN
    }

    body = json.dumps(body)
    print("request body --> ", body)
    print("url --> ", url)
    response = requests.request("POST", url, headers=headers, data=body)
    print(f"response : status -> {response.status_code}, json -> {response.json()}")

    return response

def fb_dashboard_api_update_or_delete_metadata(body: dict):
    url = f'{BANK_CONNECT_BASE_URL}/bank-connect/v1/internal/create_or_update_fraud_metadata/'
    headers = {
        "x-api-key": API_KEY,
        "server-hash": SERVER_HASH,
        "Content-Type": "application/json",
        "Authorization": SUPERUSER_TOKEN
    }

    body = json.dumps(body)
    print("request body --> ", body)
    print("url --> ", url)
    response = requests.request("POST", url, headers=headers, data=body)
    print(f"response : status -> {response.status_code}, json -> {response.json()}")

    return response

def fb_dashboard_api_update_cc_password(body: dict):
    url = f'{BANK_CONNECT_BASE_URL}/bank-connect/v1/internal/update_password_types/'
    headers = {
        "x-api-key": API_KEY,
        "server-hash": SERVER_HASH,
        "Content-Type": "application/json",
        "Authorization": SUPERUSER_TOKEN
    }

    body = json.dumps(body)
    print("request body --> ", body)
    print("url --> ", url)
    response = requests.request("POST", url, headers=headers, data=body)
    print(f"response : status -> {response.status_code}, json -> {response.json()}")

    return response

def fb_dashboard_api_fsmlib_data(body: dict):
    url = f'{BANK_CONNECT_BASE_URL}/bank-connect/v1/internal/create_or_update_fsmlib_general_data/'
    headers = {
        "x-api-key": API_KEY,
        "server-hash": SERVER_HASH,
        "Content-Type": "application/json",
        "Authorization": SUPERUSER_TOKEN
    }

    body = json.dumps(body)
    print("request body --> ", body)
    print("url --> ", url)
    response = requests.request("POST", url, headers=headers, data=body)
    print(f"response : status -> {response.status_code}, json -> {response.json()}")

    return response