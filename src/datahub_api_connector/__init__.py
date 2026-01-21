import os
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session
import json
import requests
import datetime as dt
import logging
from time import sleep
import concurrent.futures
import threading


DEFAULT_API_URL = 'https://api.opinum.com'
DEFAULT_AUTH_URL = 'https://auth.opinum.com'
DEFAULT_SCOPE = 'datahub-api'
DEFAULT_PUSH_URL = 'https://push.opinum.com'


class ApiConnector:
    """
    A class for connection to Data Hub API

    :param environment: a dictionary with all environment variables
    :param account_id: the account id to use (for users having access to multiple tenants)
    :param retries_when_connection_failure: allows to make several attempts to have a successful query (connection issues can happen)
    """

    time_limit = 3 * 60  # Three minutes

    DEFAULT_REQUEST_TIMEOUT = 10  # seconds
    MAX_RETRIES_WHEN_CONNECTION_FAILURE = 5

    def __init__(self,
                 environment=None,
                 account_id=None,
                 retries_when_connection_failure=0,
                 seconds_between_retries=5, request_timeout=DEFAULT_REQUEST_TIMEOUT, log_level="INFO"):
        logging.basicConfig()
        logging.root.setLevel(log_level)
        
        self.environment = os.environ if environment is None else environment
        self.api_url = self.environment.get('DATAHUB_API_URL', self.environment.get('OPINUM_API_URL', DEFAULT_API_URL))
        self.auth_url = f"{self.environment.get('DATAHUB_AUTH_URL', self.environment.get('OPINUM_AUTH_URL', DEFAULT_AUTH_URL))}/realms/opinum/protocol/openid-connect/token"
        self.push_url = f"{self.environment.get('DATAHUB_PUSH_URL', self.environment.get('OPINUM_PUSH_URL', DEFAULT_PUSH_URL))}/api/data/"
        self.scope = self.environment.get('DATAHUB_SCOPE', self.environment.get('OPINUM_SCOPE', DEFAULT_SCOPE))
        self.username = self.environment.get('DATAHUB_USERNAME', self.environment.get('OPINUM_USERNAME'))
        self.password = self.environment.get('DATAHUB_PASSWORD', self.environment.get('OPINUM_PASSWORD'))
        self.client_id = self.environment.get('DATAHUB_CLIENT_ID', self.environment.get('OPINUM_CLIENT_ID'))
        self.client_secret = self.environment.get('DATAHUB_CLIENT_SECRET', self.environment.get('OPINUM_SECRET'))
        self.account_id = account_id
        self.creation_time = None
        self.token = None
        self.request_timeout = request_timeout if request_timeout and request_timeout > 0 else self.DEFAULT_REQUEST_TIMEOUT
        self.max_call_attempts = 1 + min(retries_when_connection_failure, self.MAX_RETRIES_WHEN_CONNECTION_FAILURE)
        self.seconds_between_retries = seconds_between_retries
        self._token_lock = threading.Lock()
        self._set_token()

    def _set_token(self):
        with self._token_lock:
            oauth = OAuth2Session(client=LegacyApplicationClient(client_id=self.client_id))
            args = {
                'token_url': f"{self.auth_url}",
                'scope': self.scope,
                'username': self.username,
                'password': self.password,
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'auth': None
            }
            if self.account_id is not None:
                args['account'] = self.account_id
            self.token = oauth.fetch_token(**args, timeout=self.request_timeout)
            self.creation_time = dt.datetime.now()

    @property
    def _headers(self):
        if (dt.datetime.now() - self.creation_time).total_seconds() > self.time_limit:
            self._set_token()
        return {"Content-Type": "application/json",
                "Authorization": f"Bearer {self.token['access_token']}"}

    def _process_request(self, method, url, data, **kwargs):
        attempts = 0
        error = Exception('Unknown exception')
        request_headers = self._headers
        while attempts < self.max_call_attempts:
            try:
                if data is not None:
                    data = json.dumps(data)
                params = dict()
                for k, v in kwargs.items():
                    if isinstance(v, dt.datetime):
                        v = v.strftime('%Y-%m-%dT%H:%M:%S')
                    if k == 'date_from':
                        k = 'from'

                    # some requests can be used to count items, which must be added into the header
                    # in that case, we don't want to add it as a parameter
                    if k == 'IncludeItemsCount':
                        if v:
                            request_headers['x-total-count'] = "true"
                    else:
                        params[k] = v

                response = method(url, data=data, params=params, headers=request_headers, timeout=self.request_timeout)
                response.raise_for_status()
                return response
            except (requests.exceptions.ConnectionError, AssertionError) as e:
                error = e
                attempts += 1
                logging.warning(f"Failure {attempts}")
                sleep(self.seconds_between_retries)
        if attempts == self.max_call_attempts:
            logging.error(error)
            raise error

    def get(self, endpoint, data=None, **kwargs):
        """
        Method for data query in the API

        :param endpoint: the Data Hub API endpoint
        :param data: body of the request. Should always be None for a get.
        :param kwargs: dictionary of API call parameters
        :return: the http request response
        """

        return self._process_request(requests.get,
                                     f"{self.api_url}/{endpoint}",
                                     data=data,
                                     **kwargs)

    def post(self, endpoint, data=None, **kwargs):
        """
        Method for data creation in the API

        :param endpoint: the Data Hub API endpoint
        :param data: body of the request
        :param kwargs: dictionary of API call parameters
        :return: the http request response
        """
        return self._process_request(requests.post,
                                     f"{self.api_url}/{endpoint}",
                                     data=data,
                                     **kwargs)

    def patch(self, endpoint, data=None, **kwargs):
        """
        Method for data patching in the API

        :param endpoint: the Data Hub API endpoint
        :param data: body of the request
        :param kwargs: dictionary of API call parameters; see https://jsonpatch.com/
        :return: the http request response
        """
        return self._process_request(requests.patch,
                                     f"{self.api_url}/{endpoint}",
                                     data=data,
                                     **kwargs)

    def put(self, endpoint, data=None, **kwargs):
        """
        Method for data update in the API

        :param endpoint: the Data Hub API endpoint
        :param data: body of the request
        :param kwargs: dictionary of API call parameters
        :return: the http request response
        """
        return self._process_request(requests.put,
                                     f"{self.api_url}/{endpoint}",
                                     data=data,
                                     **kwargs)

    def delete(self, endpoint, data=None, **kwargs):
        """
        Method for data deletion in the API

        :param endpoint: the Data Hub API endpoint
        :param data: body of the request
        :param kwargs: dictionary of API call parameters
        :return: the http request response
        """
        return self._process_request(requests.delete,
                                     f"{self.api_url}/{endpoint}",
                                     data=data,
                                     **kwargs)

    def push_data(self, body, operation_id: str=None, operation_timeout_sec: int=None):
        """
        Method for data push in the API

        :param body: see https://docs.opinum.com/articles/push-formats/standard-format.html
        :param operation_id: a string representing the operationId of the push; see https://docs.opinum.com/articles/push-formats/standard-format.html#ask-for-a-webhook-notification
        :param operation_timeout_sec: timeout value in seconds for the push operation (default: 60s); see https://docs.opinum.com/articles/push-formats/standard-format.html#ask-for-a-webhook-notification
        :return: the http request response
        """
        return self._process_request(requests.post,
                                     self.push_url+("?operationId="+str(operation_id) if operation_id is not None else "")+("?operationTimeoutSec="+str(operation_timeout_sec) if operation_timeout_sec is not None else ""),
                                     body)

    def push_dataframe_data(self, df, **kwargs):
        """
        Method for data push in the API using a pandas DataFrame

        :param df: a pandas dataframe with dates in ISO format in 'date' column and values in 'value' column
        :param kwargs: dictionary of API call parameters, allowing to identify the target variable (see https://docs.opinum.com/articles/push-formats/standard-format.html)
        :return: the http request response
        """
        kwargs['data'] = df.to_dict('records')
        return self.push_data([kwargs])

    def send_file_to_storage(self, filename, file_io, mime_type):
        """
        Method for sending a file to the storage

        :param filename: The file name you want to give in the storage
        :param file_io: a Bytes IO or a file opened in binary
        :param mime_type: The file MIME Type
        :return: the http request response
        """
        return requests.post(f"{self.api_url}/storage?filename={filename}",
                             files={'data': (filename, file_io, mime_type)},
                             headers={"Authorization": self._headers['Authorization']})


def default_response_callback(response):
    return response


def multi_thread_request_on_path(method, endpoint,
                                 split_parameter, max_parameter_entities, max_futures, workers=16,
                                 response_callback=default_response_callback,
                                 **kwargs):
    """

    :param method: The method to use. Most used is api_connector.get where api_connector is an instance of ApiConnector
    :param endpoint: The endpoint
    :param split_parameter: The parameter having a list as input that we will split in smaller calls
    :param max_parameter_entities: The maximum number of parameters in each separate call. Mostly driven by the limit in length of the url on a http get
    :param max_futures: Preparing at once all threads is not optimal. We better loop on several groups of calls
    :param workers: The number of parallel threads. default: 16
    :param response_callback: a method with a requests response as input returning what you expect. default: a method returning the response as is
    :param kwargs: the list of http parameters
    :return: a generator returning the results of your response_callback
    """
    futures = list()
    entities = kwargs[split_parameter]
    future_run_entities = max_parameter_entities * max_futures
    with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
        for block in [entities[i: i + future_run_entities] for i in range(0, len(entities), future_run_entities)]:
            for sub_block in [block[i: i + max_futures] for i in range(0, len(block), max_futures)]:
                run_args = kwargs.copy()
                run_args[split_parameter] = sub_block
                futures.append(executor.submit(method, endpoint, **run_args))
            while True:
                all_finished = True
                for i, future in enumerate(futures):
                    if future.done():
                        yield response_callback(future.result())
                        futures.pop(i)
                    else:
                        all_finished = False
                if all_finished:
                    break

