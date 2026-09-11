"""
Offline tests for datahub_api_connector.

No network is involved: OAuth2Session is replaced by a mock, and the connector's
session verbs are replaced by recorders. ApiConnector fetches a token in its
constructor, so the OAuth2Session mock is needed by every test.
"""
import base64
import concurrent.futures
import datetime as dt
import itertools
import json
import logging
import threading
import time
from unittest import mock

import pytest
import requests
from oauthlib.oauth2 import InvalidGrantError, MissingTokenError

import datahub_api_connector as dac
from datahub_api_connector import ApiConnector, multi_thread_request_on_path


def _raise(error):
    """Raises from inside a lambda, to script a mock that fails every time."""
    raise error


ENVIRONMENT = {
    'DATAHUB_USERNAME': 'user',
    'DATAHUB_PASSWORD': 'password',
    'DATAHUB_CLIENT_ID': 'client',
    'DATAHUB_CLIENT_SECRET': 'secret',
}


def make_token(expires_in=3600, access_token='access-0', refresh_token='refresh-0'):
    return {'access_token': access_token,
            'refresh_token': refresh_token,
            'expires_at': dt.datetime.now().timestamp() + expires_in}


def make_jwt(claims):
    """A JWT-shaped access token. Only its payload matters, the signature is never verified."""
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode('utf-8')).decode('utf-8').rstrip('=')
    return f"header.{payload}.signature"


class FakeResponse:
    def __init__(self, status_code=200, text='', headers=None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or dict()

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code}", response=self)


class Recorder:
    """Stands in for a session verb, replaying a scripted list of outcomes."""

    def __init__(self, outcomes=()):
        self.outcomes = list(outcomes)
        self.calls = list()

    def __call__(self, url, **kwargs):
        self.calls.append(dict(kwargs, url=url))
        outcome = self.outcomes.pop(0) if self.outcomes else FakeResponse()
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@pytest.fixture
def oauth_session():
    with mock.patch.object(dac, 'OAuth2Session') as factory:
        # The connector uses the session as a context manager, to close it. A
        # real Session returns itself from __enter__, so the mock must too:
        # otherwise the scripted token lands on the factory's return value while
        # the code under test talks to whatever __enter__ handed back.
        factory.return_value.__enter__.return_value = factory.return_value
        factory.return_value.fetch_token.return_value = make_token()
        factory.return_value.refresh_token.return_value = make_token(access_token='access-refreshed')
        yield factory


@pytest.fixture
def no_sleep(monkeypatch):
    """Records the waits instead of serving them, and drops the backoff jitter
    so the recorded durations are predictable."""
    sleeps = list()
    monkeypatch.setattr(dac, 'sleep', sleeps.append)
    monkeypatch.setattr(dac.random, 'uniform', lambda low, high: 0.0)
    return sleeps


@pytest.fixture
def connector(oauth_session):
    with ApiConnector(environment=ENVIRONMENT) as instance:
        yield instance


@pytest.mark.parametrize('first_outcome', [requests.exceptions.ConnectionError('boom'),
                                           FakeResponse(500)],
                         ids=['connection_error', 'server_error'])
def test_body_is_serialized_once_across_retries(oauth_session, no_sleep, first_outcome):
    """A retried call used to re-encode an already-JSON body, producing a 400."""
    connector = ApiConnector(environment=ENVIRONMENT,
                             retries_when_connection_failure=1,
                             retry_unsafe_methods=True)
    recorder = Recorder([first_outcome, FakeResponse()])
    connector._session.post = recorder

    body = [{'variableId': 7, 'data': [{'date': '2026-07-30T00:00:00Z', 'value': 1.5}]}]
    connector.post('data', data=body)

    assert len(recorder.calls) == 2
    sent = [call['data'] for call in recorder.calls]
    assert sent[0] == sent[1]
    assert json.loads(sent[0]) == body


def test_attempts_and_waits_are_accounted(oauth_session, no_sleep):
    connector = ApiConnector(environment=ENVIRONMENT,
                             retries_when_connection_failure=2,
                             seconds_between_retries=3)
    failure = requests.exceptions.ConnectionError('down')
    recorder = Recorder([failure, failure, failure])
    connector._session.get = recorder

    with pytest.raises(requests.exceptions.ConnectionError):
        connector.get('sources')

    assert len(recorder.calls) == 3
    # Doubling between attempts, and no wait after the last failed one.
    assert no_sleep == [3, 6]


def test_read_timeout_is_retried(oauth_session, no_sleep):
    """ReadTimeout is not a ConnectionError, so it needs to be caught explicitly."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=1)
    recorder = Recorder([requests.exceptions.ReadTimeout('slow'), FakeResponse()])
    connector._session.get = recorder

    assert connector.get('sources').status_code == 200
    assert len(recorder.calls) == 2


def test_server_error_is_retried_on_get(oauth_session, no_sleep):
    """A transient 500 used to abort the call: HTTPError was in no except clause."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=1)
    recorder = Recorder([FakeResponse(500), FakeResponse()])
    connector._session.get = recorder

    assert connector.get('sources').status_code == 200
    assert len(recorder.calls) == 2


@pytest.mark.parametrize('status', sorted(dac.DEFAULT_RETRY_STATUSES))
def test_every_default_status_is_retried_on_a_plain_connector(oauth_session, no_sleep, status):
    """retry_on_status used to be unreachable: the default attempt budget was 1,
    so a GET 500 was raised without ever being retried."""
    connector = ApiConnector(environment=ENVIRONMENT)
    recorder = Recorder([FakeResponse(status), FakeResponse()])
    connector._session.get = recorder

    assert connector.get('sources').status_code == 200
    assert len(recorder.calls) == 2


def test_no_retry_is_still_available_explicitly(oauth_session, no_sleep):
    """0 must keep meaning one attempt; only an unset value gets the default."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=0)
    recorder = Recorder([FakeResponse(500), FakeResponse()])
    connector._session.get = recorder

    with pytest.raises(requests.exceptions.HTTPError):
        connector.get('sources')

    assert connector.max_call_attempts == 1
    assert len(recorder.calls) == 1


@pytest.mark.parametrize('failure', [requests.exceptions.ChunkedEncodingError('truncated'),
                                     requests.exceptions.ContentDecodingError('corrupt gzip')],
                         ids=['chunked_encoding', 'content_decoding'])
def test_a_read_cut_short_is_retried(oauth_session, no_sleep, failure):
    """Neither derives from requests' HTTPError, so both fell through every clause."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=1)
    recorder = Recorder([failure, FakeResponse()])
    connector._session.get = recorder

    assert connector.get('sources').status_code == 200
    assert len(recorder.calls) == 2


def test_a_timeout_is_retried_on_a_write_without_opting_in(oauth_session, no_sleep):
    """Deliberate asymmetry with the status retries: a ReadTimeout may well have
    been applied by the server, but this is what retries_when_connection_failure
    has always meant and push_data relies on it."""
    connector = ApiConnector(environment=ENVIRONMENT, retry_unsafe_methods=False)
    recorder = Recorder([requests.exceptions.ReadTimeout('slow'), FakeResponse()])
    connector._session.post = recorder

    assert connector.post('data', data={'VariableIds': [1]}).status_code == 200
    assert len(recorder.calls) == 2


def test_server_error_raises_once_the_retries_are_exhausted(oauth_session, no_sleep):
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=1)
    recorder = Recorder([FakeResponse(500), FakeResponse(503)])
    connector._session.get = recorder

    with pytest.raises(requests.exceptions.HTTPError) as raised:
        connector.get('sources')

    assert raised.value.response.status_code == 503
    assert len(recorder.calls) == 2


@pytest.mark.parametrize('status', [400, 403, 404, 409, 501])
def test_client_error_and_permanent_failure_are_not_retried(oauth_session, no_sleep, status):
    """They would fail identically on a second attempt. 501 is permanent too.
    401 is handled apart: it is replayed once with a new token."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=3)
    recorder = Recorder([FakeResponse(status), FakeResponse()])
    connector._session.get = recorder

    with pytest.raises(requests.exceptions.HTTPError):
        connector.get('sources')

    assert len(recorder.calls) == 1
    assert no_sleep == list()


def test_server_error_is_not_retried_on_post_by_default(oauth_session, no_sleep):
    """Replaying a write the server may already have applied would duplicate it."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=3)
    recorder = Recorder([FakeResponse(500), FakeResponse()])
    connector._session.post = recorder

    with pytest.raises(requests.exceptions.HTTPError):
        connector.post('sources', data={'name': 'a source'})

    assert len(recorder.calls) == 1


def test_server_error_is_retried_on_post_when_opted_in(oauth_session, no_sleep):
    """For callers whose POST only reads, such as the query-by-body POST /data."""
    connector = ApiConnector(environment=ENVIRONMENT,
                             retries_when_connection_failure=1,
                             retry_unsafe_methods=True)
    recorder = Recorder([FakeResponse(500), FakeResponse()])
    connector._session.post = recorder

    assert connector.post('data', data={'VariableIds': [1]}).status_code == 200
    assert len(recorder.calls) == 2


def test_retry_on_status_can_be_narrowed(oauth_session, no_sleep):
    connector = ApiConnector(environment=ENVIRONMENT,
                             retries_when_connection_failure=3,
                             retry_on_status={503})
    recorder = Recorder([FakeResponse(500), FakeResponse()])
    connector._session.get = recorder

    with pytest.raises(requests.exceptions.HTTPError):
        connector.get('sources')

    assert len(recorder.calls) == 1


def test_retry_on_status_can_be_disabled(oauth_session, no_sleep):
    """Restores the pre-1.4 behaviour for a caller that wants it."""
    connector = ApiConnector(environment=ENVIRONMENT,
                             retries_when_connection_failure=3,
                             retry_on_status=None)
    recorder = Recorder([FakeResponse(500), FakeResponse()])
    connector._session.get = recorder

    with pytest.raises(requests.exceptions.HTTPError):
        connector.get('sources')

    assert len(recorder.calls) == 1


def test_backoff_grows_and_is_capped(oauth_session, no_sleep):
    connector = ApiConnector(environment=ENVIRONMENT,
                             retries_when_connection_failure=5,
                             seconds_between_retries=10)
    connector._session.get = Recorder([FakeResponse(500)] * 6)

    with pytest.raises(requests.exceptions.HTTPError):
        connector.get('sources')

    assert no_sleep == [10, 20, 40, dac.MAX_BACKOFF_SECONDS, dac.MAX_BACKOFF_SECONDS]


def test_backoff_is_jittered(oauth_session, monkeypatch):
    """Without jitter, every thread sharing a connector retries in lockstep."""
    sleeps = list()
    monkeypatch.setattr(dac, 'sleep', sleeps.append)
    connector = ApiConnector(environment=ENVIRONMENT,
                             retries_when_connection_failure=1,
                             seconds_between_retries=4)
    connector._session.get = Recorder([FakeResponse(500), FakeResponse()])

    connector.get('sources')

    assert 4 <= sleeps[0] <= 5


def test_retry_after_takes_precedence_over_the_backoff(oauth_session, no_sleep):
    connector = ApiConnector(environment=ENVIRONMENT,
                             retries_when_connection_failure=1,
                             seconds_between_retries=30)
    connector._session.get = Recorder([FakeResponse(429, headers={'Retry-After': '2'}),
                                       FakeResponse()])

    connector.get('sources')

    assert no_sleep == [2]


def test_retry_after_is_capped_and_a_http_date_is_ignored(oauth_session, no_sleep):
    connector = ApiConnector(environment=ENVIRONMENT,
                             retries_when_connection_failure=2,
                             seconds_between_retries=1)
    connector._session.get = Recorder([
        FakeResponse(503, headers={'Retry-After': '99999'}),
        FakeResponse(503, headers={'Retry-After': 'Wed, 30 Jul 2026 04:00:00 GMT'}),
        FakeResponse(),
    ])

    connector.get('sources')

    # Second wait falls back to the exponential backoff of attempt 2.
    assert no_sleep == [dac.MAX_BACKOFF_SECONDS, 2]


def test_response_body_is_logged_on_server_error(oauth_session, no_sleep, caplog):
    """raise_for_status() drops the body, which is where the API states the cause."""
    connector = ApiConnector(environment=ENVIRONMENT,
                             retries_when_connection_failure=1,
                             retry_unsafe_methods=True)
    body = '{"message":"No grpc-status found on response."}'
    connector._session.post = Recorder([FakeResponse(500, text=body), FakeResponse()])

    with caplog.at_level('WARNING', logger='datahub_api_connector'):
        connector.post('data', data={'VariableIds': [1]})

    logged = caplog.text
    assert 'No grpc-status found on response.' in logged
    assert 'HTTP 500 on POST https://api.opinum.com/data' in logged


def test_a_long_response_body_is_truncated_in_the_logs(oauth_session, no_sleep, caplog):
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=0)
    connector._session.get = Recorder([FakeResponse(500, text='x' * 5000)])

    with caplog.at_level('ERROR', logger='datahub_api_connector'):
        with pytest.raises(requests.exceptions.HTTPError):
            connector.get('sources')

    assert 'x' * dac.RESPONSE_BODY_LOG_LENGTH + '...' in caplog.text
    assert 'x' * (dac.RESPONSE_BODY_LOG_LENGTH + 1) not in caplog.text


def test_the_root_logger_is_left_alone(oauth_session):
    """Configuring root reset the level of an application that had already set it."""
    logging.root.setLevel(logging.WARNING)

    ApiConnector(environment=ENVIRONMENT, log_level='DEBUG')

    assert logging.root.level == logging.WARNING
    assert logging.getLogger('datahub_api_connector').level == logging.DEBUG


def test_headers_are_rebuilt_on_each_attempt(oauth_session, no_sleep):
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=1)
    oauth_session.return_value.refresh_token.return_value = make_token(access_token='access-1')
    seen = list()

    def flaky_get(url, **kwargs):
        seen.append(kwargs['headers']['Authorization'])
        if len(seen) == 1:
            # The token expires while the first attempt is failing.
            connector.token = dict(connector.token,
                                   expires_at=dt.datetime.now().timestamp() - 1)
            raise requests.exceptions.ConnectionError('boom')
        return FakeResponse()

    connector._session.get = flaky_get
    connector.get('sources')

    assert seen == ['Bearer access-0', 'Bearer access-1']


def test_an_auth_server_hiccup_is_retried_instead_of_aborting_the_call(oauth_session, no_sleep):
    """requests_oauthlib never checks the status before parsing, so a 5xx of the
    auth server reaches us as MissingTokenError. Deriving from Exception and not
    from RequestException, it escaped both except clauses and killed the GET
    before a single HTTP attempt."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=2)
    connector.token = make_token(expires_in=-1)
    oauth_session.return_value.refresh_token.side_effect = [MissingTokenError(),
                                                           make_token(access_token='access-1')]
    oauth_session.return_value.fetch_token.side_effect = MissingTokenError()
    recorder = Recorder()
    connector._session.get = recorder

    assert connector.get('sources').status_code == 200
    assert recorder.calls[0]['headers']['Authorization'] == 'Bearer access-1'


def test_a_rejected_credential_is_not_retried(oauth_session, no_sleep):
    """oauthlib reports status_code 400 on nearly every error, so the type is the
    only usable discriminator between a dead server and a wrong password."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=3)
    connector.token = make_token(expires_in=-1)
    oauth_session.return_value.refresh_token.side_effect = InvalidGrantError()
    oauth_session.return_value.fetch_token.side_effect = InvalidGrantError()
    recorder = Recorder()
    connector._session.get = recorder

    with pytest.raises(InvalidGrantError):
        connector.get('sources')

    assert recorder.calls == list()
    assert no_sleep == list()


def test_an_unparseable_auth_response_is_reported_as_a_token_error(oauth_session, no_sleep):
    """A gateway's HTML error page comes out of the auth library as a ValueError."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=1)
    connector.token = make_token(expires_in=-1)
    oauth_session.return_value.refresh_token.side_effect = \
        ValueError('Error trying to decode a non urlencoded string')
    oauth_session.return_value.fetch_token.side_effect = \
        ValueError('Error trying to decode a non urlencoded string')

    with pytest.raises(MissingTokenError):
        connector.get('sources')


def test_a_transient_auth_failure_does_not_fail_the_constructor(oauth_session, no_sleep):
    """The constructor used to die on the very blip every other call retries."""
    oauth_session.return_value.fetch_token.side_effect = [
        requests.exceptions.ConnectionError('auth down'),
        make_token(access_token='access-late'),
    ]

    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=2)

    assert connector.token['access_token'] == 'access-late'


def test_a_rejected_credential_still_fails_the_constructor(oauth_session, no_sleep):
    oauth_session.return_value.fetch_token.side_effect = InvalidGrantError()

    with pytest.raises(InvalidGrantError):
        ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=3)

    assert oauth_session.return_value.fetch_token.call_count == 1


def test_an_auth_outage_does_not_become_a_stampede(oauth_session, no_sleep):
    """Sixteen threads sharing a connector produced a hundred and twenty-eight
    token requests, each retrying a refresh and a full re-issue on every attempt.
    A thread that finds a renewal has completed while it queued reuses its failure."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=0)
    connector.token = make_token(expires_in=-1)
    calls = list()
    counting = threading.Lock()
    together = threading.Barrier(16)

    def dead_auth(*args, **kwargs):
        with counting:
            calls.append(1)
        # Still in flight while the other threads queue on the token lock.
        time.sleep(0.05)
        raise requests.exceptions.ConnectionError('auth down')

    oauth_session.return_value.refresh_token.side_effect = dead_auth
    oauth_session.return_value.fetch_token.side_effect = dead_auth
    connector._session.get = lambda url, **kwargs: FakeResponse()

    def call():
        together.wait()
        return connector.get('sources')

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(call) for _ in range(16)]
        failed = [future for future in futures if future.exception() is not None]

    assert len(failed) == 16
    # One renewal for the whole burst: a refresh, then the full re-issue it falls
    # back to. Sixteen threads used to make sixteen of each.
    assert len(calls) == 2, f"{len(calls)} token requests for one burst of 16 threads"


def test_a_thread_retrying_in_sequence_is_never_suppressed(oauth_session, no_sleep):
    """The stampede guard must collapse simultaneous duplicates only. A thread
    coming back after its backoff has to get a real attempt, otherwise its retries
    are silently eaten."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=3)
    connector.token = make_token(expires_in=-1)
    counter = itertools.count()
    dead_auth = lambda *a, **k: (next(counter),
                                 _raise(requests.exceptions.ConnectionError('auth down')))
    oauth_session.return_value.refresh_token.side_effect = dead_auth
    oauth_session.return_value.fetch_token.side_effect = dead_auth

    with pytest.raises(requests.exceptions.ConnectionError):
        connector.get('sources')

    # A refresh and a re-issue on each of the 4 attempts: none was suppressed.
    assert next(counter) == 8
    assert len(no_sleep) == 3


def test_a_token_refused_by_the_server_is_renewed_and_the_call_replayed(oauth_session, no_sleep):
    """A revoked token, clock skew, or an account switched server-side: it looks
    perfectly valid here, so nothing used to renew it and the call failed for good."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=0)
    oauth_session.return_value.fetch_token.return_value = make_token(access_token='access-1')
    recorder = Recorder([FakeResponse(401), FakeResponse()])
    connector._session.get = recorder

    assert connector.get('sources').status_code == 200
    seen = [call['headers']['Authorization'] for call in recorder.calls]
    assert seen == ['Bearer access-0', 'Bearer access-1']
    # The replay must not eat one of the caller's attempts.
    assert no_sleep == list()


@pytest.mark.parametrize('verb', ['get', 'post', 'put', 'patch', 'delete'])
def test_the_replay_after_a_401_happens_on_every_verb(oauth_session, no_sleep, verb):
    """A 401 means the request was refused before being applied, so replaying it
    is safe even for a write and does not need retry_unsafe_methods."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=0)
    recorder = Recorder([FakeResponse(401), FakeResponse()])
    setattr(connector._session, verb, recorder)

    assert getattr(connector, verb)('sources').status_code == 200
    assert len(recorder.calls) == 2


def test_a_second_401_is_raised(oauth_session, no_sleep):
    """Otherwise a genuinely revoked account would loop on the auth server."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=3)
    recorder = Recorder([FakeResponse(401), FakeResponse(401), FakeResponse()])
    connector._session.get = recorder

    with pytest.raises(requests.exceptions.HTTPError) as raised:
        connector.get('sources')

    assert raised.value.response.status_code == 401
    assert len(recorder.calls) == 2


def test_changing_the_account_id_issues_a_token_for_it(oauth_session, no_sleep):
    """The token stayed cached until it expired, so up to an hour of calls read
    and wrote the previous tenant."""
    connector = ApiConnector(environment=ENVIRONMENT, account_id=1371)
    oauth_session.return_value.fetch_token.return_value = \
        make_token(access_token=make_jwt({'account': 1372}))
    recorder = Recorder()
    connector._session.get = recorder

    connector.account_id = 1372
    connector.get('sources')

    assert connector.token_account_id == 1372
    assert oauth_session.return_value.fetch_token.call_args.kwargs['account'] == 1372


def test_reassigning_the_same_account_id_keeps_the_token(oauth_session):
    connector = ApiConnector(environment=ENVIRONMENT, account_id=1371)
    oauth_session.return_value.fetch_token.reset_mock()

    connector.account_id = 1371

    connector._headers
    oauth_session.return_value.fetch_token.assert_not_called()


def test_a_token_carrying_another_account_is_never_sent(oauth_session, no_sleep):
    """_token_is_scoped_to_another_account() was only consulted after a refresh,
    never on the path a valid-looking token takes."""
    connector = ApiConnector(environment=ENVIRONMENT, account_id=1371)
    connector.token = make_token(access_token=make_jwt({'account': 1370}),
                                 refresh_token=None)
    oauth_session.return_value.fetch_token.return_value = \
        make_token(access_token=make_jwt({'account': 1371}))

    connector._headers

    assert connector.token_account_id == 1371


def test_a_reissued_token_on_the_wrong_account_is_reported_and_does_not_loop(oauth_session, caplog):
    """Nothing is left to try, so re-issuing on every call would only hammer the
    auth server."""
    connector = ApiConnector(environment=ENVIRONMENT, account_id=1371)
    oauth_session.return_value.refresh_token.return_value = \
        make_token(access_token=make_jwt({'account': 1370}))
    oauth_session.return_value.fetch_token.return_value = \
        make_token(access_token=make_jwt({'account': 1370}))
    connector.token = make_token(expires_in=-1)

    with caplog.at_level(logging.ERROR, logger='datahub_api_connector'):
        connector._headers
    oauth_session.return_value.fetch_token.reset_mock()
    connector._headers
    connector._headers

    assert 'Newly issued token is scoped to account 1370 instead of 1371' in caplog.text
    oauth_session.return_value.fetch_token.assert_not_called()


def test_the_claims_are_decoded_once_per_token(connector, monkeypatch):
    """They are now read on every request, to catch a changed account_id."""
    decoded = list()
    original = ApiConnector._decode_claims

    def counting_decode(access_token):
        decoded.append(access_token)
        return original(access_token)

    monkeypatch.setattr(ApiConnector, '_decode_claims', staticmethod(counting_decode))
    connector.token = make_token(access_token=make_jwt({'account': 1371}))

    assert [connector.token_account_id for _ in range(5)] == [1371] * 5
    assert len(decoded) == 1


def test_parameters_are_mapped_to_query_and_headers(connector):
    recorder = Recorder()
    connector._session.get = recorder

    connector.get('data',
                  date_from=dt.datetime(2026, 7, 30, 8, 15, 0),
                  sourceId=42,
                  IncludeItemsCount=True)

    call = recorder.calls[0]
    assert call['url'] == 'https://api.opinum.com/data'
    assert call['params'] == {'from': '2026-07-30T08:15:00', 'sourceId': 42}
    assert call['headers']['x-total-count'] == 'true'
    assert call['data'] is None
    assert call['timeout'] == ApiConnector.DEFAULT_REQUEST_TIMEOUT


def test_include_items_count_false_sets_no_header(connector):
    recorder = Recorder()
    connector._session.get = recorder

    connector.get('data', IncludeItemsCount=False)

    call = recorder.calls[0]
    assert 'x-total-count' not in call['headers']
    assert call['params'] == dict()


def test_token_expiry_honours_the_margin(connector):
    now = dt.datetime.now().timestamp()

    connector.token = {'access_token': 'a', 'expires_at': now + dac.TOKEN_EXPIRY_MARGIN + 30}
    assert not connector._token_expired()

    connector.token = {'access_token': 'a', 'expires_at': now + dac.TOKEN_EXPIRY_MARGIN - 30}
    assert connector._token_expired()

    connector.token = None
    assert connector._token_expired()


def test_expired_token_is_renewed_with_its_refresh_token(oauth_session):
    connector = ApiConnector(environment=ENVIRONMENT)
    oauth_session.return_value.fetch_token.reset_mock()
    oauth_session.return_value.refresh_token.return_value = make_token(access_token='access-1')
    connector.token = make_token(expires_in=-1)

    assert connector._headers['Authorization'] == 'Bearer access-1'
    oauth_session.return_value.fetch_token.assert_not_called()


def test_failed_refresh_falls_back_to_a_full_reissue(oauth_session):
    connector = ApiConnector(environment=ENVIRONMENT)
    oauth_session.return_value.refresh_token.side_effect = \
        requests.exceptions.HTTPError('invalid_grant')
    oauth_session.return_value.fetch_token.return_value = make_token(access_token='access-reissued')
    connector.token = make_token(expires_in=-1)

    assert connector._headers['Authorization'] == 'Bearer access-reissued'


def test_valid_token_is_not_renewed(oauth_session, connector):
    oauth_session.return_value.fetch_token.reset_mock()

    connector._headers
    connector._headers

    oauth_session.return_value.fetch_token.assert_not_called()
    oauth_session.return_value.refresh_token.assert_not_called()


def test_the_account_is_sent_again_on_a_refresh(oauth_session):
    """A refresh that omits the account could come back scoped to another one."""
    connector = ApiConnector(environment=ENVIRONMENT, account_id=1371)
    connector.token = make_token(expires_in=-1)

    connector._headers

    assert oauth_session.return_value.refresh_token.call_args.kwargs['account'] == 1371


def test_no_account_is_sent_on_a_refresh_when_the_connector_has_none(oauth_session):
    connector = ApiConnector(environment=ENVIRONMENT)
    connector.token = make_token(expires_in=-1)

    connector._headers

    assert 'account' not in oauth_session.return_value.refresh_token.call_args.kwargs


def test_a_refreshed_token_scoped_to_another_account_is_reissued(oauth_session, caplog):
    connector = ApiConnector(environment=ENVIRONMENT, account_id=1371)
    oauth_session.return_value.refresh_token.return_value = \
        make_token(access_token=make_jwt({'account': 1370}))
    oauth_session.return_value.fetch_token.return_value = \
        make_token(access_token=make_jwt({'account': 1371}))
    connector.token = make_token(expires_in=-1)

    with caplog.at_level(logging.WARNING, logger='datahub_api_connector'):
        connector._headers

    assert connector.token_account_id == 1371
    assert 'scoped to account 1370 instead of 1371' in caplog.text


def test_a_refreshed_token_without_a_readable_account_is_kept(oauth_session):
    """An opaque token is undiagnosable, not wrong, and must not cost a re-issue."""
    connector = ApiConnector(environment=ENVIRONMENT, account_id=1371)
    oauth_session.return_value.fetch_token.reset_mock()
    connector.token = make_token(expires_in=-1)

    assert connector._headers['Authorization'] == 'Bearer access-refreshed'
    oauth_session.return_value.fetch_token.assert_not_called()


@pytest.mark.parametrize('claims, expected', [({'account': 1371}, 1371),
                                              ({'accountId': '1371'}, 1371),
                                              ({'account_id': 1371}, 1371),
                                              ({'account': 'all'}, 'all'),
                                              ({'sub': 'user'}, None)])
def test_token_account_id_reads_the_account_claim(connector, claims, expected):
    connector.token = make_token(access_token=make_jwt(claims))

    assert connector.token_account_id == expected


@pytest.mark.parametrize('access_token', ['opaque', '', 'header.!!!.signature'])
def test_an_unreadable_token_has_no_claims_instead_of_raising(connector, access_token):
    connector.token = make_token(access_token=access_token)

    assert connector.token_claims == dict()
    assert connector.token_account_id is None


def test_push_data_sends_both_operation_parameters(connector):
    recorder = Recorder()
    connector._session.post = recorder

    connector.push_data([{'variableId': 1}], operation_id='op-1', operation_timeout_sec=90)

    call = recorder.calls[0]
    assert call['url'] == connector.push_url
    assert call['params'] == {'operationId': 'op-1', 'operationTimeoutSec': 90}
    assert json.loads(call['data']) == [{'variableId': 1}]


def test_push_data_without_operation_parameters(connector):
    recorder = Recorder()
    connector._session.post = recorder

    connector.push_data([{'variableId': 1}])

    assert recorder.calls[0]['params'] == dict()


def test_send_file_to_storage_encodes_the_filename(connector):
    recorder = Recorder()
    connector._session.post = recorder

    connector.send_file_to_storage('my report & draft.csv', b'a;b', 'text/csv')

    call = recorder.calls[0]
    assert call['url'] == 'https://api.opinum.com/storage'
    assert call['params'] == {'filename': 'my report & draft.csv'}
    assert call['headers'] == {'Authorization': 'Bearer access-0'}


def test_multi_thread_splits_on_max_parameter_entities():
    seen = list()
    lock = threading.Lock()

    def fake_get(endpoint, **kwargs):
        with lock:
            seen.append(kwargs['sourceId'])
        return len(kwargs['sourceId'])

    results = list(multi_thread_request_on_path(fake_get, 'data',
                                                split_parameter='sourceId',
                                                max_parameter_entities=10,
                                                max_futures=2,
                                                workers=4,
                                                sourceId=list(range(25))))

    assert sorted(len(call) for call in seen) == [5, 10, 10]
    assert sorted(entity for call in seen for entity in call) == list(range(25))
    assert sorted(results) == [5, 10, 10]


def test_multi_thread_accepts_a_bound_connector_method(connector):
    """ProcessPoolExecutor could not pickle ApiConnector (it holds a threading.Lock)."""
    recorder = Recorder()
    connector._session.get = recorder

    results = list(multi_thread_request_on_path(connector.get, 'sources',
                                                split_parameter='sourceId',
                                                max_parameter_entities=2,
                                                max_futures=2,
                                                workers=2,
                                                sourceId=[1, 2, 3]))

    assert len(results) == 2
    assert sorted(call['params']['sourceId'] for call in recorder.calls) == [[1, 2], [3]]


def test_multi_thread_propagates_failures(no_sleep):
    def failing_get(endpoint, **kwargs):
        raise requests.exceptions.ConnectionError('down')

    generator = multi_thread_request_on_path(failing_get, 'data',
                                             split_parameter='sourceId',
                                             max_parameter_entities=2,
                                             max_futures=2,
                                             sourceId=[1, 2, 3])

    with pytest.raises(requests.exceptions.ConnectionError):
        list(generator)


def _get_failing_on_zero(endpoint, **kwargs):
    if 0 in kwargs['sourceId']:
        raise requests.exceptions.HTTPError('404')
    return kwargs['sourceId']


def test_multi_thread_yields_the_successful_calls_before_raising():
    """The first failure used to abort the whole group, throwing away the calls
    the API had already answered."""
    generator = multi_thread_request_on_path(_get_failing_on_zero, 'data',
                                             split_parameter='sourceId',
                                             max_parameter_entities=1,
                                             max_futures=8, workers=8,
                                             sourceId=list(range(8)))

    yielded = list()
    with pytest.raises(requests.exceptions.HTTPError):
        for result in generator:
            yielded.append(result)

    assert sorted(yielded) == [[entity] for entity in range(1, 8)]


def test_multi_thread_can_keep_going_after_a_failure(caplog):
    generator = multi_thread_request_on_path(_get_failing_on_zero, 'data',
                                             split_parameter='sourceId',
                                             max_parameter_entities=1,
                                             max_futures=8, workers=8,
                                             raise_on_error=False,
                                             sourceId=list(range(8)))

    with caplog.at_level(logging.WARNING, logger='datahub_api_connector'):
        yielded = list(generator)

    assert sorted(yielded) == [[entity] for entity in range(1, 8)]
    assert 'A call failed in a group of 8' in caplog.text


def test_session_is_pooled_and_blocking(connector):
    adapter = connector._session.get_adapter('https://api.opinum.com')

    assert adapter._pool_block is True
    assert adapter._pool_maxsize == ApiConnector.DEFAULT_POOL_SIZE
    assert adapter.max_retries.total == 0


def test_the_auth_session_of_a_token_request_is_closed(oauth_session):
    """One OAuth2Session was left open per token request, holding a connection to
    the auth server until the garbage collector got to it."""
    ApiConnector(environment=ENVIRONMENT)

    oauth_session.return_value.__exit__.assert_called_once()


def test_the_auth_session_of_a_refresh_is_closed(oauth_session):
    connector = ApiConnector(environment=ENVIRONMENT)
    connector.token['expires_at'] = dt.datetime.now().timestamp() - 1
    oauth_session.return_value.__exit__.reset_mock()

    connector._headers

    oauth_session.return_value.refresh_token.assert_called_once()
    oauth_session.return_value.__exit__.assert_called_once()


def test_no_auth_session_is_left_open_when_a_connector_walks_many_accounts(oauth_session, no_sleep):
    """The usage pattern the docstring recommends: one connector reused across
    accounts. Neither the API session nor the auth sessions may accumulate."""
    connector = ApiConnector(environment=ENVIRONMENT, account_id=0)
    api_session = connector._session
    connector._session.get = Recorder()

    for account_id in range(1, 26):
        connector.account_id = account_id
        connector.get('sources')

    # One auth session per token request, each opened and closed in turn.
    assert oauth_session.call_count == oauth_session.return_value.__exit__.call_count
    # And a single API session throughout, rather than one per account.
    assert connector._session is api_session

    # close() must release the pool that holds the sockets. Materialised here
    # (without connecting) so the assertion is not vacuously true.
    adapter = api_session.get_adapter('https://api.opinum.com')
    adapter.poolmanager.connection_from_url('https://api.opinum.com')
    assert len(adapter.poolmanager.pools) == 1

    connector.close()
    assert len(adapter.poolmanager.pools) == 0
