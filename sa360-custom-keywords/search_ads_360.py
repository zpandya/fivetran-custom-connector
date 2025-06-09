from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests as rq
from fivetran_connector_sdk import Logging as log


import time
import requests as rq

def make_sa360_request(
    config: dict,
    method: str,
    url: str,
    session: rq.Session,
    **kwargs,
):
    """
    Makes a request to the SA360 API with the following behavior:
      - If the response is a 401 Unauthorized error and no authentication retry has occurred yet,
        it refreshes the access token and retries the request once.
      - If the response indicates a rate limit error (HTTP 429), it will retry the request using
        exponential backoff up to 5 times.
      - For all other errors, it will raise an exception.
    """
    auth_retried = False
    rate_limit_retries = 5
    # backoff factor can be adjusted; here, the delay is calculated as: delay = 2^(attempt_number - 1) seconds.
    while True:
        response = session.request(method, url, **kwargs)

        # Check for unauthorized error: refresh token only once.
        if response.status_code == 401 and not auth_retried:
            new_access_token = get_access_token(
                config["google_client_id"],
                config["google_client_secret"],
                config["google_refresh_token"],
            )
            session.headers.update({"Authorization": f"Bearer {new_access_token}"})
            auth_retried = True
            continue

        # Check for rate limit error: retry with exponential backoff.
        if response.status_code == 429 and rate_limit_retries > 0:
            # Calculate exponential delay: 1, 2, 4, 8, 16 seconds.
            attempt = 6 - rate_limit_retries
            delay = 2 ** (attempt - 1)
            time.sleep(delay)
            rate_limit_retries -= 1
            continue
        try:
            response.raise_for_status()
        except:
            print("FAILED REQUEST")
            print(url)
            print(kwargs)
            print(response.json())
        return response


def get_sa360_session(configuration: dict) -> rq.Session:
    """
    Creates a requests.Session with the initial Access Token and
    login-customer-id for Search Ads 360.
    """
    client_id = configuration["google_client_id"]
    client_secret = configuration["google_client_secret"]
    refresh_token = configuration["google_refresh_token"]
    login_customer_id = configuration["google_login_customer_id"]
    access_token = get_access_token(client_id, client_secret, refresh_token)

    session = rq.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {access_token}",
            "login-customer-id": login_customer_id,
        }
    )
    return session


def get_access_token(client_id, client_secret, refresh_token) -> str:
    """
    Exchanges a refresh token for an access token via OAuth2.
    """
    url = "https://oauth2.googleapis.com/token"
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }
    payload = "&".join([f"{key}={val}" for key, val in data.items()])
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = rq.post(url, data=payload, headers=headers)
    response.raise_for_status()
    return response.json()["access_token"]


def get_customer_clients(config: dict, session: rq.Session, submanager_id=None) -> list:
    """
    Retrieves all customer_client records (IDs, etc.) for the 'login_customer_id'
    in config. Returns a list of results.
    """
    query = "SELECT customer_client.id FROM customer_client"
    customer_id = (
        submanager_id
        if submanager_id is not None
        else config["google_login_customer_id"]
    )
    url = f"https://searchads360.googleapis.com/v0/customers/{customer_id}/searchAds360:search"

    response = make_sa360_request(
        config,
        method="POST",
        url=url,
        session=session,
        data={"query": query},
    )

    json_data = response.json()
    results = [i["customerClient"]["id"] for i in json_data.get("results", [])]
    if len(results) == 0:
        return [None]
    return results


def get_custom_columns(config, session, customer_id):
    """
    Retrieves custom columns for a particular client
    """
    url = (
        f"https://searchads360.googleapis.com/v0/customers/{customer_id}/customColumns"
    )
    response = make_sa360_request(
        config,
        method="GET",
        url=url,
        session=session,
    )
    response.raise_for_status()
    json_data = response.json()

    return json_data.get("customColumns", [])


from datetime import datetime
from dateutil.relativedelta import relativedelta

def get_custom_column_data(config, session, customer_id, custom_columns, date_cursor):
    """
    Gets historical data using the SA360 search endpoint with a pageSize of 5000.
    This generator yields each page of results from the API.
    """
    # Use the 'search' endpoint rather than 'searchStream'
    url = f"https://searchads360.googleapis.com/v0/customers/{customer_id}/searchAds360:search"
    start_date = (
        '2023-01-01'
        if date_cursor is None
        else date_cursor
    )

    # Base payload for the query with pageSize set to 5000
    payload = {
        "query": (
            f"SELECT  ad_group.id,ad_group.name, campaign.id, campaign.name, "
            f"ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type, "
            f"metrics.clicks, metrics.impressions, metrics.cost_micros, "
            f"customer.currency_code, customer.descriptive_name, segments.date, {custom_columns} "
            f"FROM keyword_view "
            f"WHERE segments.date BETWEEN '{start_date}' AND '{datetime.now().date().isoformat()}' "
            f"ORDER BY segments.date ASC"
        ),
        "pageSize": 5000,
    }
    page = 0
    next_page_token = None
    while True:
        if next_page_token:
            payload["pageToken"] = next_page_token
        else:
            payload.pop("pageToken", None)
        log.info(f"Fetching page {page}")
        response = make_sa360_request(
            config, method="POST", url=url, session=session, json=payload
        )
        response.raise_for_status()
        json_data = response.json()
        yield json_data

        # Check if a nextPageToken was provided for additional pages
        next_page_token = json_data.get("nextPageToken")
        page += 1
        if not next_page_token:
            break

def generate_custom_column_rows(config, session, customer_id, custom_columns, date_cursor):
    """
    Generator that wraps the get_custom_column_data generator.
    It iterates over each page and then over each result in the page,
    yielding a data dictionary for each custom column row.

    Parameters:
      - config: Dictionary with configuration details.
      - session: The requests.Session object.
      - customer_id: The customer ID for the API call.
      - custom_columns: Custom column fields to query.
      - date_cursor: A starting date string (or None) for the query.
    """
    # Iterate over each page of results from the SA360 search API
    for page in get_custom_column_data(config, session, customer_id, custom_columns, date_cursor):
        results = page.get("results", [])
        column_headers = page.get("customColumnHeaders", [])
        
        # Iterate over each result (row) in the page
        for record in results:
            campaign_id = record["campaign"]["id"]
            campaign_name = record["campaign"]["name"]
            clicks = record.get("metrics", {}).get("clicks", "0")
            impressions = record.get("metrics", {}).get("impressions", "0")
            cost = record.get("metrics", {}).get("costMicros", "0")
            kw_text = record.get("adGroupCriterion", {}).get("keyword", {}).get("text", "")
            kw_match_type = record.get("adGroupCriterion", {}).get("keyword", {}).get("matchType", "")
            account_name = record["customer"]["descriptiveName"]
            currency = record["customer"]["currencyCode"]
            date = record["segments"]["date"]
            custom_cols = record["customColumns"]

            # For each custom column value in the row, create a separate data dict.
            for col_val, col_header in zip(custom_cols, column_headers):
                val = col_val.get("doubleValue", None)
                column_id = col_header["id"]
                data = {
                    "column_id": column_id,
                    "value": val,
                    "date": date,
                    "campaign_id": campaign_id,
                    "customer_id": customer_id,
                    "keyword_text": kw_text,
                    "keyword_match_type": kw_match_type,
                    "campaign_name": campaign_name,
                    "account_name": account_name,
                    "currency_code": currency,
                    "clicks": clicks,
                    "impressions": impressions,
                    "cost": cost
                }
                yield data
