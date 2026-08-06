import os
import base64
from oauthlib.oauth2 import (LegacyApplicationClient, OAuth2Error, MissingTokenError,
                             InvalidGrantError, InvalidClientError, InvalidClientIdError,
                             UnauthorizedClientError, InvalidScopeError, AccessDeniedError,
                             InvalidRequestError)
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
DEFAULT_RETRY_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})

# Retries granted when the caller does not say. Without them retry_on_status was
# configured but unreachable: a single attempt never gets to a second one.
DEFAULT_RETRIES = 3

# Upper bound on the exponential backoff between two attempts.
MAX_BACKOFF_SECONDS = 60

# Number of characters of the response body kept in the retry/failure logs. The
# API puts the actual reason there (e.g. the gRPC detail behind a 500), which
# raise_for_status() does not include in its message.
RESPONSE_BODY_LOG_LENGTH = 200

# Methods that can be replayed without risk of applying a change twice.
IDEMPOTENT_VERBS = frozenset({'get'})

# Claims an access token may carry the account it is scoped to under. Read in
# this order, so a token using several of them yields a stable answer.
ACCOUNT_CLAIM_NAMES = ('account', 'accountId', 'account_id')

# A token the server refuses although it looked valid here. Replayed once with a
# fresh token instead of being retried like a server error: a wrong credential
# must not cost a full round of attempts.
UNAUTHORIZED = 401

# Failures of the auth server that a later attempt may well get through: a 5xx or
# a gateway's HTML error page reaches us as MissingTokenError, because the auth
# library parses the body without checking the status first. Everything not
# listed here is treated as transient, since oauthlib reports status_code 400 on
# nearly every error and the type is the only usable discriminator.
PERMANENT_AUTH_ERRORS = (InvalidGrantError, InvalidClientError, InvalidClientIdError,
                         UnauthorizedClientError, InvalidScopeError, AccessDeniedError,
                         InvalidRequestError)

# Transport failures worth another attempt. Retried whatever the verb is: a write
# that timed out may have been applied and replaying it could duplicate it, but
# that is the long-standing meaning of retries_when_connection_failure and
# push_data relies on it. The unsafe-method gate deliberately covers only the
# HTTP statuses, see _is_retryable_status.
# ChunkedEncodingError (truncated response) and ContentDecodingError (corrupt
# gzip) are listed explicitly: neither derives from requests' HTTPError, so
# without them a read cut short was raised instead of retried.
TRANSIENT_TRANSPORT_ERRORS = (requests.exceptions.ConnectionError,
                              requests.exceptions.Timeout,
                              requests.exceptions.ChunkedEncodingError,
                              requests.exceptions.ContentDecodingError,
                              AssertionError)


class ApiConnector:
    """
    A class for connection to Data Hub API

    :param environment: a dictionary with all environment variables
    :param account_id: the account id to use (for users having access to multiple tenants). Assigning a new one invalidates the current token, so the next call is made on the new account
    :param retries_when_connection_failure: allows to make several attempts to have a successful query (connection issues can happen). Defaults to DEFAULT_RETRIES; pass 0 for a single attempt
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
                 retries_when_connection_failure=None,
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
        # Set through the attribute rather than the property: the setter needs the
        # lock and the token, which do not exist yet.
        self._account_id = account_id
        self.token = None
        self.request_timeout = request_timeout if request_timeout and request_timeout > 0 else self.DEFAULT_REQUEST_TIMEOUT
        # None means "no opinion" and gets the default, while an explicit 0 still
        # means a single attempt.
        retries = DEFAULT_RETRIES if retries_when_connection_failure is None \
            else retries_when_connection_failure
        self.max_call_attempts = 1 + min(retries, self.MAX_RETRIES_WHEN_CONNECTION_FAILURE)
        self.seconds_between_retries = seconds_between_retries
        self.retry_on_status = frozenset(retry_on_status or ())
        self.retry_unsafe_methods = retry_unsafe_methods
        self._token_lock = threading.Lock()
        # Decoding the claims on every request would base64-decode and parse the
        # same token over and over, so the last result is kept.
        self._claims_cache = (None, dict())
        # Number of token requests made, and the failure of the last one when it
        # failed. Together they let a thread tell whether another one has just
        # asked the auth server on its behalf. See _ensure_valid_token.
        self._auth_attempts = 0
        self._auth_failure = None
        # Access token of a re-issue that came back on the wrong account anyway.
        # See _renew_token.
        self._accepted_token = None

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

        self._get_first_token()

    def _get_first_token(self):
        """
        Obtains the first token, retrying a transient failure of the auth server.

        A blip of the auth server used to make the constructor raise, although it
        is the very kind of failure every other call retries.
        """
        for attempt in range(1, self.max_call_attempts + 1):
            try:
                self._ensure_valid_token()
                return
            except PERMANENT_AUTH_ERRORS:
                raise
            except (OAuth2Error,) + TRANSIENT_TRANSPORT_ERRORS as e:
                logger.warning(f"Failure {attempt}/{self.max_call_attempts} "
                               f"while requesting the first token: {e}")
                if attempt >= self.max_call_attempts:
                    raise
                sleep(self._backoff_delay(attempt))

    @property
    def account_id(self):
        """
        The account the connector works on.

        Assigning a new one drops the current token: it is scoped to the previous
        account, and keeping it would silently read and write the wrong tenant
        until it expired.
        """
        return self._account_id

    @account_id.setter
    def account_id(self, value):
        with self._token_lock:
            if value == self._account_id:
                return
            self._account_id = value
            self.token = None
            self._accepted_token = None

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
        # The account must be sent again: a refresh grant that omits it can come
        # back scoped to another account than the one this connector works on,
        # and the caller would then read and write the wrong tenant's data with a
        # token that otherwise looks perfectly valid.
        account_args = {'account': self.account_id} if self.account_id is not None else dict()
        self.token = oauth.refresh_token(self.auth_url,
                                         client_id=self.client_id,
                                         client_secret=self.client_secret,
                                         timeout=self.request_timeout,
                                         **account_args)

    @property
    def token_claims(self) -> dict:
        """
        Payload of the current access token, or an empty dictionary when it cannot be read.

        The signature is not verified: this is our own token, obtained over TLS,
        and the claims are only used for diagnostics.
        """
        access_token = self.token.get('access_token') if self.token else None
        if not access_token:
            return dict()
        # Read once: another thread may replace the cache between the two lines.
        cached_for, cached_claims = self._claims_cache
        if cached_for == access_token:
            return cached_claims
        claims = self._decode_claims(access_token)
        self._claims_cache = (access_token, claims)
        return claims

    @staticmethod
    def _decode_claims(access_token):
        try:
            payload = access_token.split('.')[1]
            # base64url without its padding, which b64decode requires.
            payload += '=' * (-len(payload) % 4)
            return json.loads(base64.urlsafe_b64decode(payload).decode('utf-8'))
        except Exception:
            # An opaque or malformed token is not an error here, just undiagnosable.
            return dict()

    @property
    def token_account_id(self):
        """
        The account the current token is scoped to, as claimed by the token itself.

        None when the claim is absent, which is also the case for a token issued
        without an account. Useful to check that a call really is going to hit
        the account the connector was built for.
        """
        claims = self.token_claims
        for name in ACCOUNT_CLAIM_NAMES:
            if name in claims:
                value = claims[name]
                try:
                    return int(value)
                except (TypeError, ValueError):
                    return value
        return None

    def _token_is_scoped_to_another_account(self):
        """True only when the token positively claims an account other than ours."""
        if self.account_id is None:
            return False
        token_account_id = self.token_account_id
        return token_account_id is not None and token_account_id != self.account_id

    def _token_expired(self):
        expires_at = self.token.get('expires_at') if self.token else None
        if expires_at is None:
            return True
        return dt.datetime.now().timestamp() >= expires_at - TOKEN_EXPIRY_MARGIN

    def _needs_renewal(self):
        """True when the token must be replaced before it is sent again."""
        if self._token_expired():
            return True
        if self.token and self.token.get('access_token') == self._accepted_token:
            # A token the auth server will not scope correctly, already accepted
            # once. Re-issuing on every call would only hammer it.
            return False
        # Catches an account_id changed at runtime, whose token still carries the
        # previous tenant. Sending it would read and write the wrong account.
        return self._token_is_scoped_to_another_account()

    def _ensure_valid_token(self):
        """
        Renews the token when needed and returns the one to send.

        Returns it rather than leaving the caller to read self.token afterwards:
        another thread can drop it in between (see _invalidate_token) and the
        caller would then send no token at all.
        """
        if not self._needs_renewal():
            token = self.token
            if token:
                return token
            # Dropped between the check and the read; fall through to the lock,
            # under which nothing can change it.
        # Read before queueing on the lock, so it tells how many token requests
        # had been made by the time this thread started waiting.
        attempts_before_waiting = self._auth_attempts
        with self._token_lock:
            # Another thread may have already renewed the token while this one
            # was waiting for the lock.
            if not self._needs_renewal():
                return self.token
            if self._auth_failure is not None and self._auth_attempts > attempts_before_waiting:
                # Another thread asked the auth server while this one queued, and
                # it failed. Asking again right now would only add load: an auth
                # outage used to turn into a stampede, sixteen threads sharing a
                # connector producing a hundred and twenty-eight token requests.
                # A thread coming back after its backoff has seen the counter
                # move, so its own retry is never suppressed.
                raise self._auth_failure
            self._renew_token()
            return self.token

    def _renew_token(self):
        """Replaces the token. The caller must hold self._token_lock."""
        try:
            if self.token and self.token.get('refresh_token'):
                try:
                    self._request_token(refresh=True)
                    # A server that ignores the account of a refresh grant would
                    # hand back a valid token on the wrong tenant. Rather than
                    # trust it, fall through to a full re-issue, which always
                    # carries the account.
                    if not self._token_is_scoped_to_another_account():
                        self._auth_failure = None
                        return
                    logger.warning(f"Refreshed token is scoped to account {self.token_account_id} "
                                   f"instead of {self.account_id}, requesting a new one")
                except Exception:
                    logger.warning("Token refresh failed, falling back to a full reissue", exc_info=True)
            self._request_token(refresh=False)
        except Exception as e:
            self._auth_failure = e
            raise
        finally:
            # Counted once the attempt is over, not when it starts: a thread that
            # queues while a renewal is still in flight must see the count it had
            # before, so it reuses the outcome instead of asking in turn.
            self._auth_attempts += 1
        self._auth_failure = None
        if self._token_is_scoped_to_another_account():
            # A full re-issue always carries the account, so there is nothing
            # left to try. The token is kept rather than re-issued on every
            # single call, and the caller is told loudly.
            logger.error(f"Newly issued token is scoped to account {self.token_account_id} "
                         f"instead of {self.account_id}")
            self._accepted_token = self.token.get('access_token')

    def _request_token(self, refresh):
        """Calls the auth server, reporting an unreadable response as an OAuth2Error."""
        try:
            if refresh:
                self._refresh_token()
            else:
                self._set_token()
        except ValueError as e:
            # A body the auth library cannot parse at all, such as a gateway's
            # HTML error page, surfaces as a ValueError from its url decoding.
            # Reported as a token error so the retry loop can classify it.
            raise MissingTokenError(description=str(e)) from e

    def _invalidate_token(self, stale_token):
        """
        Drops the token so the next call asks for a new one.

        Only the token that just failed is dropped: another thread may have
        renewed it meanwhile, and clearing that one would throw away a good token.
        """
        with self._token_lock:
            if self.token is stale_token:
                self.token = None
                self._accepted_token = None

    def _request_headers(self, include_items_count=False):
        """Headers for one attempt, with the token they carry (see _invalidate_token)."""
        token = self._ensure_valid_token()
        headers = {"Content-Type": "application/json",
                   "Authorization": f"Bearer {token['access_token']}"}
        if include_items_count:
            headers['x-total-count'] = "true"
        return headers, token

    @property
    def _headers(self):
        headers, _ = self._request_headers()
        return headers

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

        # Token of the attempt in flight, so a 401 invalidates the one that was
        # actually refused rather than whatever is cached by then.
        sent_with = None

        def send():
            nonlocal sent_with
            # Headers are rebuilt on every attempt so that a token renewed
            # between two attempts is actually used.
            request_headers, sent_with = self._request_headers(include_items_count)
            response = method(url, data=body, params=params,
                              headers=request_headers, timeout=self.request_timeout)
            response.raise_for_status()
            return response

        error = Exception('Unknown exception')
        failure = str(error)
        reauthenticated = False
        for attempt in range(1, self.max_call_attempts + 1):
            try:
                try:
                    return send()
                except requests.exceptions.HTTPError as e:
                    if reauthenticated or getattr(e.response, 'status_code', None) != UNAUTHORIZED:
                        raise
                    # The server refused a token this side still considered valid:
                    # revoked, clock skew, or an account switched behind our back.
                    # It never applied the request, so replaying it once with a
                    # fresh token is safe for any verb, and it does not consume one
                    # of the caller's attempts. A second 401 is raised below.
                    reauthenticated = True
                    logger.warning(f"HTTP {UNAUTHORIZED} on {verb.upper()} {url},"
                                   f" renewing the token and replaying")
                    self._invalidate_token(sent_with)
                    return send()
            except PERMANENT_AUTH_ERRORS:
                # A rejected credential or scope fails identically next time.
                raise
            except OAuth2Error as e:
                # The auth server could not be reached or could not be understood.
                # Getting a token has no effect on the resource, so this is retried
                # whatever the verb is.
                error = e
                failure = f"Token request failed: {e}"
                delay = self._backoff_delay(attempt)
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
            except TRANSIENT_TRANSPORT_ERRORS as e:
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
                                 raise_on_error=True,
                                 **kwargs):
    """

    :param method: The method to use. Most used is api_connector.get where api_connector is an instance of ApiConnector
    :param endpoint: The endpoint
    :param split_parameter: The parameter having a list as input that we will split in smaller calls
    :param max_parameter_entities: The maximum number of parameters in each separate call. Mostly driven by the limit in length of the url on a http get
    :param max_futures: Preparing at once all threads is not optimal. We better loop on several groups of calls
    :param workers: The number of parallel threads. default: 16. Must not exceed the pool_size of your ApiConnector
    :param response_callback: a method with a requests response as input returning what you expect. default: a method returning the response as is
    :param raise_on_error: raise the first failure of a group once its successful calls have been yielded. default: True. Set to False to get what worked and only log the rest
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
            failures = list()
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                except Exception as e:
                    # Raising straight away discarded the calls of the group that
                    # had already succeeded, throwing away work the API had done.
                    # They are yielded first, then the failure is reported.
                    logger.warning(f"A call failed in a group of {len(futures)}: {e}")
                    failures.append(e)
                    continue
                yield response_callback(result)
            if failures and raise_on_error:
                raise failures[0]

