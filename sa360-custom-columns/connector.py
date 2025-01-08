from datetime import datetime
import json
import requests as rq
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
from search_ads_360 import (
    get_sa360_session,
    get_customer_clients,
    get_custom_columns,
    get_custom_column_data,
)


# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
def schema(configuration: dict):
    return [
        {
            "table": "custom_columns",
            "primary_key": ["column_id", "customer_id"],
            "columns": {
                "customer_id": "STRING",
                "column_id": "STRING",
                "description": "STRING",
                "name": "STRING",
                "render_type": "STRING",
                "value_type": "STRING",
            },
        },
        {
            "table": "custom_column_values",
            "primary_key": ["column_id", "customer_id", "campaign_id", "date"],
            "columns": {
                "date": "NAIVE_DATE",
                "column_id": "STRING",
                "campaign_id": "STRING",
                "customer_id": "STRING",
                "value": "STRING",
            },
        },
    ]


def get_date_diff(date_str_1, date_str_2):
    fmt = "%Y-%m-%d"
    d1 = datetime.strptime(date_str_1, fmt)
    d2 = datetime.strptime(date_str_2, fmt)
    return (d2 - d1).days


# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
# - state: a dictionary containing the state checkpointed during the prior sync.
#   The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: dict, state: dict):
    session = get_sa360_session(configuration)

    column_data_cursor = state.get("column_data_cursor", None)
    iterative_sync_cursor = state.get("iterative_sync_cursor", None)

    # if "curr" in state and "remaining" in state:
    #     curr = state["curr"]
    #     remaining = state["remaining"]
    # else:
    #     curr, *remaining = ['1115771062', '9134099894']

    submanager_accounts = ["1115771062", "9134099894"]
    submanager_accounts.sort(key=lambda x: int(x))
    submanager_cursor = state.get("submanager_cursor", submanager_accounts[0])
    managed_account_cursor = state.get("managed_account_cursor", None)
    for account in submanager_accounts:

        # increment cursor when we get to a new submanager account
        if managed_account_cursor is None:
            yield op.checkpoint(
                {
                    "submanager_cursor": account,
                    "iterative_sync_cursor": iterative_sync_cursor,
                    "column_data_cursor": column_data_cursor,
                }
            )
        else:
            yield op.checkpoint(
                {
                    "submanager_cursor": account,
                    "managed_account_cursor": managed_account_cursor,
                    "iterative_sync_cursor": iterative_sync_cursor,
                    "column_data_cursor": column_data_cursor,
                }
            )
        if int(account) < int(submanager_cursor):
            continue

        columns = get_custom_columns(configuration, session, account)
        managed_accounts = get_customer_clients(configuration, session, account)
        managed_accounts.sort(key=lambda x: int(x))
        managed_account_cursor = state.get(
            "managed_account_cursor", managed_accounts[0]
        )

        for a in managed_accounts:

            # increment cursor when we get to a new managed account
            yield op.checkpoint(
                {
                    "submanager_cursor": account,
                    "managed_account_cursor": a,
                    "iterative_sync_cursor": iterative_sync_cursor,
                    "column_data_cursor": column_data_cursor,
                }
            )

            if int(a) < int(managed_account_cursor):
                continue

            # sync columns
            for column in columns:
                data = {
                    "customer_id": a,
                    "column_id": column["id"],
                    "description": column.get("description", ""),
                    "name": column["name"],
                    "render_type": column["renderType"],
                    "value_type": column["valueType"],
                }
                yield op.upsert(table="custom_columns", data=data)

            print(f"Getting data for account {a}")
            if len(columns) > 0:
                column_fields = ",".join(
                    [f"custom_columns.id[{i['id']}]" for i in columns]
                )

                start_date = (
                    column_data_cursor
                    if iterative_sync_cursor is None
                    else iterative_sync_cursor
                )
                column_data = get_custom_column_data(
                    configuration, session, a, column_fields, start_date
                )
                if len(column_data) > 0:
                    results = column_data[0].get("results", [])
                    column_headers = column_data[0].get("customColumnHeaders", [])
                    _start_date = None
                    for i in results:
                        campaign_id = i["campaign"]["id"]
                        date = i["segments"]["date"]
                        custom_columns = i["customColumns"]

                        if _start_date is None:
                            _start_date = date
                            yield op.checkpoint(
                                {
                                    "submanager_cursor": account,
                                    "managed_account_cursor": a,
                                    "iterative_sync_cursor": iterative_sync_cursor,
                                    "column_data_cursor": date,
                                }
                            )
                        elif get_date_diff(_start_date, date) < 0:
                            continue
                        elif get_date_diff(_start_date, date) > 5:
                            _start_date = date
                            yield op.checkpoint(
                                {
                                    "submanager_cursor": account,
                                    "managed_account_cursor": a,
                                    "iterative_sync_cursor": iterative_sync_cursor,
                                    "column_data_cursor": date,
                                }
                            )
                        for column_value, column in zip(custom_columns, column_headers):
                            val = column_value.get("doubleValue", None)
                            column_id = column["id"]
                            data = {
                                "column_id": column_id,
                                "value": val,
                                "date": date,
                                "campaign_id": campaign_id,
                                "customer_id": a,
                            }
                            yield op.upsert(table="custom_column_values", data=data)

    yield op.checkpoint(
        {
            "iterative_sync_cursor": datetime.now().date().isoformat(),
        }
    )
    # while remaining is not None:
    #     print(f"Client {curr} has {len(columns)} custom columns")

    #     columns = get_custom_columns(configuration, session, curr)
    #     for column in columns:
    #         data = {
    #             "customer_id": curr,
    #             "column_id": column["id"],
    #             "description": column.get("description", ""),
    #             "name": column["name"],
    #             "render_type": column["renderType"],
    #             "value_type": column["valueType"]
    #         }
    #         yield op.upsert(table="custom_columns", data=data)

    #     if len(columns) > 0:
    #         column_fields = ','.join([f"custom_columns.id[{i['id']}]" for i in columns])

    #         start_date = date_cursor if iterative_date_cursor is None else iterative_date_cursor
    #         column_data = get_custom_column_data(configuration, session, curr, column_fields, start_date)
    #         if len(column_data) > 0:
    #             results = column_data[0].get("results",[])
    #             column_headers = column_data[0].get("customColumnHeaders",[])
    #             for i in results:
    #                 campaign_id = i["campaign"]["id"]
    #                 date = i["segments"]["date"]
    #                 if iterative_date_cursor is not None:
    #                     pass
    #                 else:
    #                     if date_cursor is None:
    #                         date_cursor = date
    #                         yield op.checkpoint({"curr": curr, "remaining": remaining, "date_cursor": date_cursor})
    #                     elif get_date_diff(date_cursor, date) < 0:
    #                         continue
    #                     elif get_date_diff(date_cursor, date) > 2:
    #                         date_cursor = date
    #                         yield op.checkpoint({"curr": curr, "remaining": remaining, "date_cursor": date_cursor})

    #                 custom_columns = i["customColumns"]
    #                 for column_value, column in zip(custom_columns, column_headers):
    #                     val = column_value.get("doubleValue", None)
    #                     column_id = column["id"]
    #                     data = {
    #                         "column_id": column_id,
    #                         "value": val,
    #                         "date": date,
    #                         "campaign_id" : campaign_id,
    #                         "customer_id": curr
    #                     }
    #                     yield op.upsert(table="custom_column_values", data=data)

    #     if len(remaining) == 0:
    #         curr = None
    #         remaining = None
    #     else:
    #         curr, *remaining = remaining

    #     date_cursor = None
    #     yield op.checkpoint(state={
    #         "curr": curr,
    #         "remaining": remaining
    #     })
    # yield op.checkpoint({
    #     "iterative_date_cursor": datetime.now().date().isoformat()
    # })

    # print(curr, remaining, columns)


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug()

# Resulting table:
# ┌───────────────────────────┬─────────────────┬───────────────────────────┬─────────────┐
# │         starttime         │      name       │          endtime          │ temperature │
# │ timestamp with time zone  │     varchar     │ timestamp with time zone  │    int16    │
# ├───────────────────────────┼─────────────────┼───────────────────────────┼─────────────┤
# │ 2024-08-06 14:30:00+05:30 │ Overnight       │ 2024-08-06 15:30:00+05:30 │          77 │
# │ 2024-08-06 15:30:00+05:30 │ Tuesday         │ 2024-08-07 03:30:00+05:30 │          82 │
# │ 2024-08-07 03:30:00+05:30 │ Tuesday Night   │ 2024-08-07 15:30:00+05:30 │          78 │
# │ 2024-08-07 15:30:00+05:30 │ Wednesday       │ 2024-08-08 03:30:00+05:30 │          83 │
# │ 2024-08-08 03:30:00+05:30 │ Wednesday Night │ 2024-08-08 15:30:00+05:30 │          78 │
# │ 2024-08-08 15:30:00+05:30 │ Thursday        │ 2024-08-09 03:30:00+05:30 │          82 │
# │ 2024-08-09 03:30:00+05:30 │ Thursday Night  │ 2024-08-09 15:30:00+05:30 │          78 │
# │ 2024-08-09 15:30:00+05:30 │ Friday          │ 2024-08-10 03:30:00+05:30 │          84 │
# │ 2024-08-10 03:30:00+05:30 │ Friday Night    │ 2024-08-10 15:30:00+05:30 │          78 │
# │ 2024-08-10 15:30:00+05:30 │ Saturday        │ 2024-08-11 03:30:00+05:30 │          85 │
# │ 2024-08-11 03:30:00+05:30 │ Saturday Night  │ 2024-08-11 15:30:00+05:30 │          76 │
# │ 2024-08-11 15:30:00+05:30 │ Sunday          │ 2024-08-12 03:30:00+05:30 │          83 │
# │ 2024-08-12 03:30:00+05:30 │ Sunday Night    │ 2024-08-12 15:30:00+05:30 │          74 │
# │ 2024-08-12 15:30:00+05:30 │ Monday          │ 2024-08-13 03:30:00+05:30 │          84 │
# ├───────────────────────────┴─────────────────┴───────────────────────────┴─────────────┤
# │ 14 rows                                                                     4 columns │
# └───────────────────────────────────────────────────────────────────────────────────────┘
