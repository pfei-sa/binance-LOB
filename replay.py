from datetime import datetime
from typing import Dict, Generator, List, Optional, Tuple
from infi.clickhouse_orm.database import Database
from model import DiffDepthStream, DepthSnapshot
from clickhouse_driver import Client
from config import CONFIG
from tqdm import tqdm
import heapq


def diff_depth_stream_generator(
    last_update_id: int, symbol: str, block_size: Optional[int] = None
) -> Generator[
    Tuple[datetime, int, int, List[float], List[float], List[float], List[float], str],
    None,
    None,
]:
    database = CONFIG.db_name
    db = Database(CONFIG.db_name, db_url=f"http://{CONFIG.host_name}:8123/")
    client = Client(host=CONFIG.host_name)
    client.execute(f"USE {database}")
    qs = (
        DiffDepthStream.objects_in(db)
        .filter(
            DiffDepthStream.symbol == symbol.upper(),
            DiffDepthStream.final_update_id >= last_update_id,
        )
        .order_by("timestamp")
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
) -> Generator[Tuple[datetime, int, Dict[float, float], Dict[float, float], str], None, None]:
    database = CONFIG.db_name
    db = Database(CONFIG.db_name, db_url=f"http://{CONFIG.host_name}:8123/")
    client = Client(host=CONFIG.host_name)
    client.execute(f"USE {database}")

    qs = (
        DepthSnapshot.objects_in(db).filter(
            DepthSnapshot.symbol == symbol.upper(),
            DepthSnapshot.last_update_id > last_update_id,
        )
    ).order_by("timestamp")

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
    for diff_stream in diff_depth_stream_generator(last_update_id, symbol, block_size):
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


def partial_orderbook_generator(
    last_update_id: int, symbol: str, level: int = 10, block_size: Optional[int] = None
) -> Generator[Tuple[datetime, int, List[float], str], None, None]:
    database = CONFIG.db_name
    db = Database(CONFIG.db_name)
    client = Client(host=CONFIG.host_name)
    client.execute(f"USE {database}")

    qs = (
        DepthSnapshot.objects_in(db).filter(
            DepthSnapshot.symbol == symbol.upper(),
            DepthSnapshot.last_update_id > last_update_id,
        )
    ).order_by("timestamp")

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

    bids_levels = heapq.nlargest(level, bids_book.keys())
    asks_levels = heapq.nsmallest(level, asks_book.keys())

    bids_levels.sort(reverse=True)
    asks_levels.sort()

    result = [
        val
        for tup in zip(
            *[
                bids_levels,
                [bids_book[p] for p in bids_levels],
                asks_levels,
                [asks_book[p] for p in asks_levels],
            ]
        )
        for val in tup
    ]

    yield (timestamp, last_update_id, result, symbol)
    prev_final_update_id = None
    for diff_stream in diff_depth_stream_generator(last_update_id, symbol, block_size):
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

        bids_levels = heapq.nlargest(level * 30, bids_book.keys())
        asks_levels = heapq.nsmallest(level * 30, asks_book.keys())

        bids_book = {p: bids_book[p] for p in bids_levels}
        asks_book = {p: asks_book[p] for p in asks_levels}

        bids_levels = heapq.nlargest(level, bids_levels)
        asks_levels = heapq.nsmallest(level, asks_levels)

        bids_levels.sort(reverse=True)
        asks_levels.sort()

        result = [
            val
            for tup in zip(
                *[
                    bids_levels,
                    [bids_book[p] for p in bids_levels],
                    asks_levels,
                    [asks_book[p] for p in asks_levels],
                ]
            )
            for val in tup
        ]

        yield (timestamp, final_update_id, result, symbol)


def lists_to_dict(price: List[float], quantity: List[float]) -> Dict[float, float]:
    return {p: q for p, q in zip(price, quantity)}


def update_book(
    book: Dict[float, float], price: List[float], quantity: List[float]
) -> None:
    for p, q in zip(price, quantity):
        if q == 0:
            book.pop(p, 0)
        else:
            book[p] = q


def get_snapshots_update_ids(symbol: str) -> List[int]:
    database = CONFIG.db_name
    client = Client(host=CONFIG.host_name)
    client.execute(f"USE {database}")
    return client.execute(
        f"SELECT last_update_id FROM depthsnapshot WHERE symbol = '{symbol.upper()}' ORDER BY "
        "timestamp"
    )


if __name__ == "__main__":
    i = 0
    first_id = last_id = 0
    for r in tqdm(partial_orderbook_generator(0, "ETHUSDT")):
        i += 1
        if i >= 1_000:
            break
    print(i)
    print(r)
    print(get_snapshots_update_ids("ETHUSDT"))
