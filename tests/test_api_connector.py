"""
Offline tests for datahub_api_connector.

No network is involved: OAuth2Session is replaced by a mock, and the connector's
session verbs are replaced by recorders. ApiConnector fetches a token in its
constructor, so the OAuth2Session mock is needed by every test.
"""
import datetime as dt
import json
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


class FakeResponse:
    def __init__(self, status_code=200):
        self.status_code = status_code

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
    sleeps = list()
    monkeypatch.setattr(dac, 'sleep', sleeps.append)
    return sleeps


@pytest.fixture
def connector(oauth_session):
    with ApiConnector(environment=ENVIRONMENT) as instance:
        yield instance


def test_body_is_serialized_once_across_retries(oauth_session, no_sleep):
    """A retried call used to re-encode an already-JSON body, producing a 400."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=1)
    recorder = Recorder([requests.exceptions.ConnectionError('boom'), FakeResponse()])
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
    # No wait after the last failed attempt.
    assert no_sleep == [3, 3]


def test_read_timeout_is_retried(oauth_session, no_sleep):
    """ReadTimeout is not a ConnectionError, so it needs to be caught explicitly."""
    connector = ApiConnector(environment=ENVIRONMENT, retries_when_connection_failure=1)
    recorder = Recorder([requests.exceptions.ReadTimeout('slow'), FakeResponse()])
    connector._session.get = recorder

    assert connector.get('sources').status_code == 200
    assert len(recorder.calls) == 2


def test_http_error_is_not_retried(connector):
    recorder = Recorder([FakeResponse(404), FakeResponse()])
    connector._session.get = recorder

    with pytest.raises(requests.exceptions.HTTPError):
        connector.get('sources')

    assert len(recorder.calls) == 1


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
