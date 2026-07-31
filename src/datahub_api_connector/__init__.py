import os
from oauthlib.oauth2 import LegacyApplicationClient
from requests_oauthlib import OAuth2Session
import json
import requests
from requests.adapters import HTTPAdapter
import datetime as dt
import logging
import random
from time import sleep
import concurrent.futures
import threading


logger = logging.getLogger(__name__)

DEFAULT_API_URL = 'https://api.opinum.com'
DEFAULT_AUTH_URL = 'https://auth.opinum.com'
DEFAULT_SCOPE = 'datahub-api'
DEFAULT_PUSH_URL = 'https://push.opinum.com'

# Safety buffer applied to the token's real expiry (from the OAuth2 token
# response) so a request can't start with a token that expires mid-flight.
TOKEN_EXPIRY_MARGIN = 120

# Statuses worth retrying: the server told us it failed, but a later attempt may
# well succeed. 501 (Not Implemented) is deliberately absent, it is permanent.
DEFAULT_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})

# Upper bound on the exponential backoff between two attempts.
MAX_BACKOFF_SECONDS = 60

# Number of characters of the response body kept in the retry/failure logs. The
# API puts the actual reason there (e.g. the gRPC detail behind a 500), which
# raise_for_status() does not include in its message.
RESPONSE_BODY_LOG_LENGTH = 200

# Methods that can be replayed without risk of applying a change twice.
IDEMPOTENT_VERBS = frozenset({'get'})


class ApiConnector:
    """
    A class for connection to Data Hub API

    :param environment: a dictionary with all environment variables
    :param account_id: the account id to use (for users having access to multiple tenants)
    :param retries_when_connection_failure: allows to make several attempts to have a successful query (connection issues can happen)
    :param retry_on_status: HTTP statuses that are retried instead of raised immediately
    :param retry_unsafe_methods: also retry those statuses on POST/PUT/PATCH/DELETE, not only on GET
    """

    DEFAULT_REQUEST_TIMEOUT = 10  # seconds
    MAX_RETRIES_WHEN_CONNECTION_FAILURE = 5

    # Size of the connection pool of the persistent session. It must be at least
    # as large as the number of concurrent threads sharing an ApiConnector
    # instance (see the workers parameter of multi_thread_request_on_path).
    DEFAULT_POOL_SIZE = 32

    def __init__(self,
                 environment=None,
                 account_id=None,
                 retries_when_connection_failure=0,
                 seconds_between_retries=5, request_timeout=DEFAULT_REQUEST_TIMEOUT, log_level="INFO",
                 pool_size=DEFAULT_POOL_SIZE,
                 retry_on_status=DEFAULT_RETRY_STATUSES,
                 retry_unsafe_methods=False):
        # Only this package's logger is configured. Calling logging.basicConfig()
        # and logging.root.setLevel() reset the level of an application that had
        # already configured logging itself before building an ApiConnector.
        logger.setLevel(log_level)

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
        self.token = None
        self.request_timeout = request_timeout if request_timeout and request_timeout > 0 else self.DEFAULT_REQUEST_TIMEOUT
        self.max_call_attempts = 1 + min(retries_when_connection_failure, self.MAX_RETRIES_WHEN_CONNECTION_FAILURE)
        self.seconds_between_retries = seconds_between_retries
        self.retry_on_status = frozenset(retry_on_status or ())
        self.retry_unsafe_methods = retry_unsafe_methods
        self._token_lock = threading.Lock()

        # Persistent session with a bounded, blocking connection pool. Without it
        # every call opened its own connection; under multi_thread_request_on_path
        # those sockets piled up in TIME_WAIT until the OS ran out of ephemeral
        # ports (ConnectionError / WinError 10055 on long runs). pool_block=True
        # makes a thread wait for a free connection instead of opening a
        # throwaway one.
        self._session = requests.Session()
        adapter = HTTPAdapter(pool_connections=pool_size,
                              pool_maxsize=pool_size,
                              pool_block=True,
                              max_retries=0)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

        self._ensure_valid_token()

    def _set_token(self):
        """Request a brand new token. The caller must hold self._token_lock."""
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

    def _refresh_token(self):
        """Renew the current token with its refresh token. The caller must hold self._token_lock."""
        oauth = OAuth2Session(client=LegacyApplicationClient(client_id=self.client_id),
                              token=self.token)
        self.token = oauth.refresh_token(self.auth_url,
                                         client_id=self.client_id,
                                         client_secret=self.client_secret,
                                         timeout=self.request_timeout)

    def _token_expired(self):
        expires_at = self.token.get('expires_at') if self.token else None
        if expires_at is None:
            return True
        return dt.datetime.now().timestamp() >= expires_at - TOKEN_EXPIRY_MARGIN

    def _ensure_valid_token(self):
        if not self._token_expired():
            return
        with self._token_lock:
            # Another thread may have already renewed the token while this one
            # was waiting for the lock.
            if not self._token_expired():
                return
            if self.token and self.token.get('refresh_token'):
                try:
                    self._refresh_token()
                    return
                except Exception:
                    logger.warning("Token refresh failed, falling back to a full reissue", exc_info=True)
            self._set_token()

    @property
    def _headers(self):
        self._ensure_valid_token()
        return {"Content-Type": "application/json",
                "Authorization": f"Bearer {self.token['access_token']}"}

    def _backoff_delay(self, attempt, response=None):
        """
        Seconds to wait before the next attempt: exponential, capped, with jitter.

        A fixed delay made all the threads sharing a connector retry in lockstep
        after a common upstream hiccup, which hit the recovering service with the
        very same burst that had just failed. The jitter spreads them out.
        A Retry-After given by the server takes precedence.
        """
        retry_after = self._retry_after_seconds(response)
        if retry_after is not None:
            return min(retry_after, MAX_BACKOFF_SECONDS)
        delay = min(self.seconds_between_retries * 2 ** (attempt - 1), MAX_BACKOFF_SECONDS)
        return delay + random.uniform(0, delay * 0.25)

    @staticmethod
    def _retry_after_seconds(response):
        """Retry-After in its delay-seconds form; None when absent or a HTTP date."""
        if response is None:
            return None
        raw = getattr(response, 'headers', None) or {}
        try:
            return max(0.0, float(raw.get('Retry-After')))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _body_snippet(response):
        """Beginning of the response body, for logging. Never raises."""
        try:
            text = response.text or ''
        except Exception:
            return ''
        text = ' '.join(text.split())
        if len(text) > RESPONSE_BODY_LOG_LENGTH:
            return f"{text[:RESPONSE_BODY_LOG_LENGTH]}..."
        return text

    def _is_retryable_status(self, verb, response):
        # An HTTPError raised without a response carries no status to judge on.
        if response is None or getattr(response, 'status_code', None) not in self.retry_on_status:
            return False
        # Replaying a POST/PUT/PATCH/DELETE that the server may have already
        # applied before failing would duplicate the change, so those need an
        # explicit opt-in. It is safe for calls that only read, such as the
        # query-by-body POST /data.
        return self.retry_unsafe_methods or verb in IDEMPOTENT_VERBS

    def _process_request(self, method, url, data, verb='get', **kwargs):
        # The body is serialized once, outside the retry loop. Serializing it
        # inside meant a retried call re-encoded an already-JSON string and sent
        # a doubly-encoded body, which the API rejects with a 400.
        body = json.dumps(data) if data is not None else None

        params = dict()
        include_items_count = False
        for k, v in kwargs.items():
            if isinstance(v, dt.datetime):
                v = v.strftime('%Y-%m-%dT%H:%M:%S')
            if k == 'date_from':
                k = 'from'

            # some requests can be used to count items, which must be added into the header
            # in that case, we don't want to add it as a parameter
            if k == 'IncludeItemsCount':
                include_items_count = bool(v)
            else:
                params[k] = v

        error = Exception('Unknown exception')
        failure = str(error)
        for attempt in range(1, self.max_call_attempts + 1):
            try:
                # Headers are rebuilt on every attempt so that a token renewed
                # between two attempts is actually used.
                request_headers = dict(self._headers)
                if include_items_count:
                    request_headers['x-total-count'] = "true"

                response = method(url, data=body, params=params,
                                  headers=request_headers, timeout=self.request_timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.HTTPError as e:
                # raise_for_status() turns any 4xx/5xx into an HTTPError. A status
                # the server may recover from is retried; anything else (400, 401,
                # 404, ...) would fail again identically, so it is raised at once.
                response = e.response
                if not self._is_retryable_status(verb, response):
                    raise
                error = e
                failure = (f"HTTP {response.status_code} on {verb.upper()} {url}"
                           f" - {self._body_snippet(response)}")
                delay = self._backoff_delay(attempt, response)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    AssertionError) as e:
                error = e
                failure = str(e)
                delay = self._backoff_delay(attempt)

            logger.warning(f"Failure {attempt}/{self.max_call_attempts}: {failure}")
            if attempt < self.max_call_attempts:
                sleep(delay)
        logger.error(f"Giving up after {self.max_call_attempts} attempts: {failure}")
        raise error

    def get(self, endpoint, data=None, **kwargs):
        """
        Method for data query in the API

        :param endpoint: the Data Hub API endpoint
        :param data: body of the request. Should always be None for a get.
        :param kwargs: dictionary of API call parameters
        :return: the http request response
        """

        return self._process_request(self._session.get,
                                     f"{self.api_url}/{endpoint}",
                                     data=data,
                                     verb='get',
                                     **kwargs)

    def post(self, endpoint, data=None, **kwargs):
        """
        Method for data creation in the API

        :param endpoint: the Data Hub API endpoint
        :param data: body of the request
        :param kwargs: dictionary of API call parameters
        :return: the http request response
        """
        return self._process_request(self._session.post,
                                     f"{self.api_url}/{endpoint}",
                                     data=data,
                                     verb='post',
                                     **kwargs)

    def patch(self, endpoint, data=None, **kwargs):
        """
        Method for data patching in the API

        :param endpoint: the Data Hub API endpoint
        :param data: body of the request
        :param kwargs: dictionary of API call parameters; see https://jsonpatch.com/
        :return: the http request response
        """
        return self._process_request(self._session.patch,
                                     f"{self.api_url}/{endpoint}",
                                     data=data,
                                     verb='patch',
                                     **kwargs)

    def put(self, endpoint, data=None, **kwargs):
        """
        Method for data update in the API

        :param endpoint: the Data Hub API endpoint
        :param data: body of the request
        :param kwargs: dictionary of API call parameters
        :return: the http request response
        """
        return self._process_request(self._session.put,
                                     f"{self.api_url}/{endpoint}",
                                     data=data,
                                     verb='put',
                                     **kwargs)

    def delete(self, endpoint, data=None, **kwargs):
        """
        Method for data deletion in the API

        :param endpoint: the Data Hub API endpoint
        :param data: body of the request
        :param kwargs: dictionary of API call parameters
        :return: the http request response
        """
        return self._process_request(self._session.delete,
                                     f"{self.api_url}/{endpoint}",
                                     data=data,
                                     verb='delete',
                                     **kwargs)

    def push_data(self, body, operation_id: str=None, operation_timeout_sec: int=None):
        """
        Method for data push in the API

        :param body: see https://docs.opinum.com/articles/push-formats/standard-format.html
        :param operation_id: a string representing the operationId of the push; see https://docs.opinum.com/articles/push-formats/standard-format.html#ask-for-a-webhook-notification
        :param operation_timeout_sec: timeout value in seconds for the push operation (default: 60s); see https://docs.opinum.com/articles/push-formats/standard-format.html#ask-for-a-webhook-notification
        :return: the http request response
        """
        # Both parameters are passed as query parameters. Concatenating them into
        # the url used to produce "?operationId=x?operationTimeoutSec=y", where
        # the second "?" made the timeout part of the operationId value.
        params = dict()
        if operation_id is not None:
            params['operationId'] = operation_id
        if operation_timeout_sec is not None:
            params['operationTimeoutSec'] = operation_timeout_sec
        return self._process_request(self._session.post,
                                     self.push_url,
                                     body,
                                     verb='post',
                                     **params)

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
        return self._session.post(f"{self.api_url}/storage",
                                  params={'filename': filename},
                                  files={'data': (filename, file_io, mime_type)},
                                  headers={"Authorization": self._headers['Authorization']},
                                  timeout=self.request_timeout)

    def close(self):
        """
        Releases the connections held by the underlying session.

        ApiConnector can also be used as a context manager, which closes it on exit.
        """
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False


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
    :param workers: The number of parallel threads. default: 16. Must not exceed the pool_size of your ApiConnector
    :param response_callback: a method with a requests response as input returning what you expect. default: a method returning the response as is
    :param kwargs: the list of http parameters
    :return: a generator returning the results of your response_callback
    """
    entities = kwargs[split_parameter]
    # One call per group of max_parameter_entities entities, then at most
    # max_futures of those calls in flight at any time.
    calls = [entities[i: i + max_parameter_entities]
             for i in range(0, len(entities), max_parameter_entities)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        for batch in [calls[i: i + max_futures] for i in range(0, len(calls), max_futures)]:
            futures = list()
            for sub_block in batch:
                run_args = kwargs.copy()
                run_args[split_parameter] = sub_block
                futures.append(executor.submit(method, endpoint, **run_args))
            for future in concurrent.futures.as_completed(futures):
                yield response_callback(future.result())

