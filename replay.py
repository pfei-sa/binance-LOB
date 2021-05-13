from datetime import datetime
from typing import Dict, Generator, List, Optional, Tuple
from infi.clickhouse_orm.database import Database
from model import DiffDepthStream, DepthSnapshot
from clickhouse_driver import Client
from config import CONFIG


def diff_depth_stream_generator(
    last_update_id: int, symbol: str, block_size: Optional[int] = None
) -> Generator[
    Tuple[datetime, int, int, List[float], List[float], List[float], List[float], str],
    None,
    None,
]:
    database = CONFIG.db_name
    db = Database(database)
    client = Client(host="localhost")
    client.execute(f"USE {database}")
    qs = (
        DiffDepthStream.objects_in(db)
        .filter(
            DiffDepthStream.symbol == symbol.upper(),
            DiffDepthStream.final_update_id >= last_update_id,
        )
        .order_by("final_update_id")
    )

    if block_size is None:
        for row in client.execute(qs.as_sql()):
            yield row
    else:
        settings = {"max_block_size": block_size}
        rows_gen = client.execute_iter(qs.as_sql(), settings=settings)
        for row in rows_gen:
            yield row


def orderbook_generator(
    last_update_id: int, symbol: str, block_size: Optional[int] = None
):
    database = CONFIG.db_name
    db = Database(database)
    client = Client(host="localhost")
    client.execute(f"USE {database}")

    qs = (
        DepthSnapshot.objects_in(db).filter(
            DepthSnapshot.symbol == symbol.upper(),
            DepthSnapshot.last_update_id > last_update_id,
        )
    ).order_by(last_update_id)

    result = client.execute(qs.as_sql())
    if len(result) == 0:
        return
    snapshot = result[0]
    client.disconnect()
    (
        timestamp,
        last_update_id,
        bids_quantity,
        bids_price,
        asks_quantity,
        asks_price,
        _,
    ) = snapshot

    bids_book = lists_to_dict(bids_price, bids_quantity)
    asks_book = lists_to_dict(asks_price, asks_quantity)

    yield (timestamp, last_update_id, bids_book.copy(), asks_book.copy(), symbol)
    prev_final_update_id = None
    for diff_stream in diff_depth_stream_generator(
        last_update_id, symbol, block_size
    ):
        # https://binance-docs.github.io/apidocs/spot/en/#how-to-manage-a-local-order-book-correctly
        (
            timestamp,
            first_update_id,
            final_update_id,
            diff_bids_quantity,
            diff_bids_price,
            diff_asks_quantity,
            diff_asks_price,
            _,
        ) = diff_stream

        if (
            prev_final_update_id is not None
            and prev_final_update_id + 1 != first_update_id
        ):
            return
        prev_final_update_id = final_update_id

        if prev_final_update_id is None and (
            last_update_id + 1 < first_update_id or last_update_id + 1 > final_update_id
        ):
            raise ValueError()

        update_book(bids_book, diff_bids_price, diff_bids_quantity)
        update_book(asks_book, diff_asks_price, diff_asks_quantity)

        yield (timestamp, final_update_id, bids_book.copy(), asks_book.copy(), symbol)


def lists_to_dict(price, quantity):
    return {p: q for p, q in zip(price, quantity)}


def update_book(book: Dict, price, quantity):
    for p, q in zip(price, quantity):
        if q == 0:
            book.pop(p, 0)
        else:
            book[p] = q


def get_snapshots_update_ids(symbol: str) -> int:
    database = CONFIG.db_name
    client = Client(host="localhost")
    client.execute(f"USE {database}")
    return client.execute(
        f"SELECT last_update_id FROM depthsnapshot WHERE symbol = '{symbol.upper()}'"
    )


if __name__ == "__main__":
    i = 0
    first_id = last_id = 0
    for r in orderbook_generator(7505673970, "ETHUSDT"):
        i += 1
    print(r[2])
    print(i)
