from src.datahub_api_connector import ApiConnector as api

# GET sources examples (with and without count)
# response = api(account_id=920, request_timeout=5, log_level="INFO").get('sources', DisplayLevel='Light', IncludeItemsCount=True)
# nb_sources = response.headers.get('x-total-count', 'unknown')
# sources = response.json()
# print(f"Found {len(sources)} sources")

# sources = api(account_id=920, request_timeout=5, log_level="INFO").get('sources', DisplayLevel='Light').json()
# print(f"Found {len(sources)} sources")


# PUSH data example
# push_data = [
#     {
#         "variableId": 7174702,
#         "data": [
#             {"date": "2025-07-01T00:00:00","value": 70}, {"date": "2025-06-01T00:00:00","value": 60},
#             {"date": "2025-05-01T00:00:00","value": 50}, {"date": "2025-04-01T00:00:00","value": 40},
#             {"date": "2025-03-01T00:00:00","value": 30}, {"date": "2025-02-01T00:00:00","value": 20},
#             {"date": "2025-01-01T00:00:00","value": 10}
#         ]
#     }
# ]

# response = api(account_id=920, request_timeout=5, log_level="INFO").push_data(push_data, "test_push_2026_01_21")
# print(response)