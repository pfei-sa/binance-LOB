# Binance Limited Orderbook Recorder
This data recorder obtains orderbook datastream using Binance API (both with websocket and snapshot) and data are entered into a clickhouse instance for faster analysis and reconstruction of local orderbook from data streamed.

## WARNING
All price and quantity information are stored as `Float64` as this project was written primarliy for data exploration/machine learning purpose. This means and price/quantity information are not acurate and should not be used in production environment.

## Getting Started

### Prerequisite

#### Docker
To start data collection, make sure `docker` and `docker-compose` is install on the system. Although it should be possible to run without docker however it is not tested.

#### Config file
Modify config.json to control the behavior of the data collection process:
```jsonc
{
    // symbols to track
    "symbols": [
        // Spot symbols
        "ethusdt",
        "btcusdt",
        "dogeusdt",
        // USD-M Futures needs 'USD_' prefix
        "USD_btcusdt",
        // COIN-M Futures needs 'COIN_' prefix
        "COIN_btcusd_perp"
    ],
    // interval to refetch full orderbook snapshot (seconds)
    "full_fetch_interval": 3600,
    // level of orderbook to fetch for snapshot
    // for detail see binance api GET /api/v3/depth
    "full_fetch_limit": 1000,
    // update speed to listen for diff depth stream
    "stream_interval": 100,
    // control if logging information are logged to console
    "log_to_console": true,
    // default database name
    "db_name": "archive",
    // host name of the clickhouse instance if in docker
    "host_name_docker": "clickhouse",
    // host name of the clickhouse instance if not in docker
    "host_name_default": "localhost"
}
```
#### Replay
This repo also include few utility function for reconstructing full orderbook from diff depth stream. They are included in `replay.py`. If you want to use it, also install all the required libraries with `pip install -r requirements.txt`.

## Starting Data Collection
To start collecting data, first build image for the main python script with `docker-compose build`. Then, to start both python script and clickhouse instance, use `docker-compose up`. Clickhouse ports `8123` and `9000` are exposed to `localhost`.

## Database structure
To see how data are stored in the database, see `DepthSnapshot` and `DiffDepthStream` in `model.py`.

## Replaying orderbook

Orderbook is reconstructed by following instruction on [binance api page](https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream). To use included replay functionality, first use `get_snapshots_update_ids(symbol)` to get list of snapshot ids for the given symbol. Then use `orderbook_generator` (or `partial_orderbook_generator` if you are only intered in a partial orderbook) with `last_update_id` = the id of snapshot you are instered to reconstruct from minus 1. Here is an example on how you should use these generator.

```python
for r in orderbook_generator(0, "ETHUSDT", block_size=5000):
    # here last_update_id=0 is supplied to contruct orderbook
    # from first avalible snapshot.
    # block_size should also be used if the database is really large
    ... 
    #process yor orderbook
```

The generator is exhausted when there is a gap in the diff depth stream  (probably due to connection lost while logging data), i.e. the previous `final_update_id + 1 != first_update_id`, or there is no more diff stream in the database. To skip the gap and start a new generator, simply use the last `last_update_id` from previus iteration again.

## Documentation
See [documentation](https://sa-tony.github.io/binance-LOB/replay) for detail on how to use the replay modules
