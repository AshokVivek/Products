import requests
from requests.auth import AuthBase, HTTPBasicAuth, HTTPDigestAuth
from requests.adapters import HTTPAdapter, Retry
from typing import Union, Optional, Any, Dict
import json
#Maintiaing a constant request session to http connection polling
#currently this is used to call django servers from rams
REQUEST_SESSION = requests.Session()
retry_policy = Retry(total=3, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
http_adapter = HTTPAdapter(max_retries=retry_policy)
REQUEST_SESSION.mount("http://", http_adapter)
REQUEST_SESSION.mount("https://", http_adapter)

RequestFiles = Dict[str, Union[str, tuple[str, str, str]]]




def call_api_with_session(
        url: str, 
        method: str,
        payload: Optional[Dict[str, Any]] = None, 
        headers: Optional[Dict[str, str]] = None, 
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = 100,
        files: Optional[RequestFiles] = None,
        auth: Optional[Union[AuthBase, HTTPBasicAuth, HTTPDigestAuth]] = None,
        ):
    if not timeout:
        timeout = 100
    response = REQUEST_SESSION.request(
        method=method, 
        url=url, 
        data=payload, 
        headers=headers, 
        params=params, 
        timeout=timeout, 
        files=files, 
        auth=auth
    )
    return response