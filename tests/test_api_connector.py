"""
Offline tests for datahub_api_connector.

No network is involved: OAuth2Session is replaced by a mock, and the connector's
session verbs are replaced by recorders. ApiConnector fetches a token in its
constructor, so the OAuth2Session mock is needed by every test.
"""
import base64
import datetime as dt
import json
import logging
import threading
from unittest import mock

import pytest
import requests

import datahub_api_connector as dac
from datahub_api_connector import ApiConnector, multi_thread_request_on_path


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


def test_server_error_raises_once_the_retries_are_exhausted(oauth_session, no_sleep):
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=1)
    recorder = Recorder([FakeResponse(500), FakeResponse(503)])
    connector._session.get = recorder

    with pytest.raises(requests.exceptions.HTTPError) as raised:
        connector.get('sources')

    assert raised.value.response.status_code == 503
    assert len(recorder.calls) == 2


@pytest.mark.parametrize('status', [400, 401, 403, 404, 409, 501])
def test_client_error_and_permanent_failure_are_not_retried(oauth_session, no_sleep, status):
    """They would fail identically on a second attempt. 501 is permanent too."""
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


def test_session_is_pooled_and_blocking(connector):
    adapter = connector._session.get_adapter('https://api.opinum.com')

    assert adapter._pool_block is True
    assert adapter._pool_maxsize == ApiConnector.DEFAULT_POOL_SIZE
    assert adapter.max_retries.total == 0
