### Fivetran Custom Connector

Connector created using [fivetran docs for the connector sdk](https://fivetran.com/docs/connectors/connector-sdk/setup-guide). Straightforward weather api querying, from the [fivetran connector sdk example repository](https://github.com/fivetran/fivetran_connector_sdk/tree/main)

1. Setup python env
2. install fivetran-connector-sdk
3. run `fivetran debug` or `python connector.py` from ~/connector to run locally
4. run `fivetran deploy` with your api key, destination, and desired connector name to deploy (example in github workflow)
