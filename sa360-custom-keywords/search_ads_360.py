from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests as rq


def make_sa360_request(
    config: dict,
    method: str,
    url: str,
    session: rq.Session,
    max_retries: int = 1,
    **kwargs,
):
    """Makes a request to the SA360 API, refreshing token if needed."""

    response = session.request(method, url, **kwargs)

    # If unauthorized, try to refresh token and retry
    if response.status_code == 401 and max_retries > 0:
        new_access_token = get_access_token(
            config["google_client_id"],
            config["google_client_secret"],
            config["google_refresh_token"],
        )
        session.headers.update({"Authorization": f"Bearer {new_access_token}"})
        return make_sa360_request(config, method, url, session, max_retries=0, **kwargs)

    response.raise_for_status()
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


def get_custom_column_data(config, session, customer_id, custom_columns, date_cursor):
    """
    Gets historical data
    """

    url = f"https://searchads360.googleapis.com/v0/customers/{customer_id}/searchAds360:searchStream"
    start_date = (
        (datetime.now() - relativedelta(years=2)).date().isoformat()
        if date_cursor is None
        else date_cursor
    )
    payload = {
        "query": f"SELECT campaign.id,  campaign.name, ad_group_criterion.keyword.text, ad_group_criterion.keyword.match_type, metrics.clicks, metrics.impressions, metrics.cost_micros, customer.currency_code, customer.descriptive_name, segments.date, {custom_columns} FROM keyword_view WHERE segments.date BETWEEN '{start_date}' AND '{datetime.now().date().isoformat()}' ORDER BY segments.date ASC"
    }
    response = make_sa360_request(
        config, method="POST", url=url, session=session, data=payload
    )
    response.raise_for_status()
    json_data = response.json()
    return json_data
