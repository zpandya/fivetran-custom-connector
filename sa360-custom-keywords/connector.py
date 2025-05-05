from datetime import datetime
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
from search_ads_360 import (
    generate_custom_column_rows,
    get_sa360_session,
    get_customer_clients,
    get_custom_columns,
    get_custom_column_data,
)


# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
def schema(configuration: dict):
    return [
        {
            "table": "custom_column_metrics",
            "primary_key": [
                "column_id",
                "customer_id",
                "campaign_id",
                "date",
                "keyword_text",
                "keyword_match_type",
            ],
            "columns": {
                "date": "NAIVE_DATE",
                "column_id": "STRING",
                "campaign_id": "STRING",
                "customer_id": "STRING",
                "value": "STRING",
                "keyword_text": "STRING",
                "keyword_match_type": "STRING",
                "campaign_name": "STRING",
                "account_name": "STRING",
                "currency_code": "STRING",
                "clicks": "STRING",
                "impressions": "STRING",
                "cost": "STRING",
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
    log.info("Connector started successfully.")
    session = get_sa360_session(configuration)

    column_data_cursor = state.get("column_data_cursor", None)
    iterative_sync_cursor = state.get("iterative_sync_cursor", None)

    submanager_accounts = list(
        map(
            lambda z: z.strip(),
            configuration.get("submanager_account_ids", "").split(","),
        )
    )
    submanager_accounts.sort(key=lambda x: int(x))
    submanager_cursor = state.get("submanager_cursor", submanager_accounts[0])
    managed_account_cursor = state.get("managed_account_cursor", None)
    for account in submanager_accounts:
        log.info(f"Beginning sync for submanager {account}")
        if int(account) < int(submanager_cursor):
            continue

        columns = get_custom_columns(configuration, session, account)
        managed_accounts = list(
            filter(
                lambda z: z not in submanager_accounts,
                get_customer_clients(configuration, session, account),
            )
        )
        managed_accounts.sort(key=lambda x: int(x))
        managed_account_cursor = state.get(
            "managed_account_cursor", managed_accounts[0]
        )

        for a in managed_accounts:
            log.info(f"Beginning sync for account {a}")
            if int(a) < int(managed_account_cursor):
                continue

            if len(columns) > 0:
                column_fields = ",".join(
                    [f"custom_columns.id[{i['id']}]" for i in columns]
                )

                start_date = (
                    column_data_cursor
                    if iterative_sync_cursor is None
                    else iterative_sync_cursor
                )
                log.info("Beginning fetch")
                for idx, item in enumerate(
                    generate_custom_column_rows(
                        configuration, session, a, column_fields, start_date
                    )
                ):

                    if idx % 10000 == 0:
                        log.info(f"Processed {idx} records -- {item["date"]}")
                    if idx % 50000 == 0 and idx != 0:
                        log.info(f"Checkpoint at {idx} records -- {item["date"]}")
                        yield op.checkpoint(
                            {
                                "submanager_cursor": account,
                                "managed_account_cursor": a,
                                "iterative_sync_cursor": iterative_sync_cursor,
                                "column_data_cursor": item["date"],
                            }
                        )

                    yield op.upsert(table="custom_column_metrics", data=item)

    yield op.checkpoint(
        {
            "iterative_sync_cursor": datetime.now().date().isoformat(),
        }
    )


connector = Connector(update=update, schema=schema)
if __name__ == "__main__":
    connector.debug()
