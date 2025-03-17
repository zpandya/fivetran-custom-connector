from datetime import datetime
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
def update(configuration: dict, state: dict):
    session = get_sa360_session(configuration)

    column_data_cursor = state.get("column_data_cursor", None)
    iterative_sync_cursor = state.get("iterative_sync_cursor", None)

    submanager_accounts = list(map(lambda z: z.strip(), configuration.get("submanager_account_ids", "").split(",")))
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


connector = Connector(update=update, schema=schema)
if __name__ == "__main__":
    connector.debug()
