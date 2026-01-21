This package simplifies the calls to Data Hub's API.

No magic. You need to follow the [Data Hub Swagger Documentation](https://api.opinum.com) for correct formatting of your requests

Be aware that this is a new package, certainly missing many features. Feel free to contribute.

You first need to create an instance of the ApiConnector class with following parameters:

*ATTENTION:* this package can only be used from Data Hub version 7.0 (July 1, 2025) onward, as it uses the authentication to the the new technology (Keycloak).
For previous versions, the opinum-api-connector package must be used (https://github.com/opinum/opinum-api-connector) instead.

*VERSION 1.2*
Fixed operation_id on PUSH calls, and added operation_timeout_sec. Also added the possibility to request a count of items on some calls with the IncludeItemsCount optional parameter.

*VERSION 1.1*
Improved sturdiness. Added thread lock on token requests, and a default timeout of 10 seconds on all requests (incliding token)

> _environment_
> > a dictionary of environment variables
> >
> > if `None`, ApiConnector uses your environment variables (_os.environ_)
> >
> > Mandatory environment variables are:
> >
> > * _DATAHUB_USERNAME_: the Data Hub user. <br>
> > TAKE CARE: if this user has access to multiple tenants and if you do not specify a tenant id,
> > ApiConnector will use the last tenant used.
> > * _DATAHUB_PASSWORD_: the password for the user
> > * _DATAHUB_CLIENT_ID_: the client id for accessing the API
> > * _DATAHUB_CLIENT_SECRET_ the corresponding secret
> > 
> > Optional environment variables are:
> >
> > * _DATAHUB_API_URL_: another API URL than the Europe SaaS one (https://api.opinum.com)
> > * _DATAHUB_AUTH_URL_: another authentication URL than the Europe SaaS one (https://auth.opinum.com)
> > * _DATAHUB_PUSH_URL_: another push URL than the Europe SaaS one (https://push.opinum.com)
> > * _DATAHUB_SCOPE_: the scope of you session (default: "_datahub-api_")<br>
> > if you want to push data, the scope should be "_datahub-api push-data_"

> _account_id_
> > one of the tenant ids available for the Data Hub user (default: `None`)

> _retries_when_connection_failure_
> > number of extra attempts when no 200 or 204 return code (default: 0, maximum: 5)

> _request_timeout_
> > timeout value in seconds on all requests (including fetch token) (default: 10)

> _log_level_
> > sets log level for the module (default: INFO)

Once you have your ApiConnector instance, you may use the class methods

* get
* post
* patch
* put
* delete
* send_file_to_storage

All keyword arguments will be converted to path parameters in the API call with one important exception,
the _data_ keyword referring to the body of your call.

There are two other class methods for data pushing because we have another API for this

* push_data
* push_dataframe_data

There is a little bit of magic with the method multi_thread_request_on_path that splits a list of parameters
Allowing to make parallel calls.
