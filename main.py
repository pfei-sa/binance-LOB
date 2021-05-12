from asyncio.events import AbstractEventLoop
from typing import List
from aiohttp.client import ClientSession
from infi.clickhouse_orm.database import Database
from model import DepthSnapshot, DiffDepthStreamDispatcher, Logger, LoggingLevel
from datetime import datetime
import asyncio
import aiohttp
from pydantic import BaseModel, ValidationError
from time import time
from config import Config


CONFIG = Config()


class DiffDepthStreamMsg(BaseModel):
    e: str  # Event type
    E: int  # Event time (Unix Epoch ms)
    s: str  # Symbol
    U: int  # start update id
    u: int  # end update id
    b: List[List[float]]  # bids [price, quantity]
    a: List[List[float]]  # asks [price, quantity]


class DepthSnapshotMsg(BaseModel):
    lastUpdateId: int
    bids: List[List[float]]
    asks: List[List[float]]


def depth_stream_url(symbol: str) -> str:
    speed = CONFIG.stream_interval
    assert speed in (1000, 100), "speed must be 1000 or 100"
    symbol = symbol.lower()
    endpoint = f"{symbol}@depth" if speed == 1000 else f"{symbol}@depth@100ms"
    return f"wss://stream.binance.com:9443/ws/{endpoint}"


async def get_full_depth(symbol: str, session: ClientSession, database: Database):
    limit = CONFIG.full_fetch_limit
    url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit={limit}"
    async with session.get(url) as resp:
        resp_json = await resp.json()
        msg = DepthSnapshotMsg(**resp_json)
        snapshot = DepthSnapshot(
            timestamp=datetime.utcnow(),
            last_update_id=msg.lastUpdateId,
            bids_quantity=[pairs[1] for pairs in msg.bids],
            bids_price=[pairs[0] for pairs in msg.bids],
            asks_quantity=[pairs[1] for pairs in msg.asks],
            asks_price=[pairs[0] for pairs in msg.asks],
            symbol=symbol,
        )
        database.insert([snapshot])


async def handle_depth_stream(
    symbol: str,
    session: ClientSession,
    dispatcher: DiffDepthStreamDispatcher,
    database: Database,
    logger: Logger,
    loop: AbstractEventLoop,
):
    next_full_fetch = time()
    logger.log_msg(f"Connecting to {symbol} stream", LoggingLevel.INFO, symbol)
    while True:
        async with session.ws_connect(depth_stream_url(symbol)) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data_raw = DiffDepthStreamMsg(**msg.json())
                    except ValidationError:
                        print(msg.data)
                        break

                    s = data_raw.E / 1000.0
                    timestamp = datetime.utcfromtimestamp(s)
                    first_update_id = data_raw.U
                    final_update_id = data_raw.u
                    bids_quantity = [pairs[1] for pairs in data_raw.b]
                    bids_price = [pairs[0] for pairs in data_raw.b]
                    asks_quantity = [pairs[1] for pairs in data_raw.a]
                    asks_price = [pairs[0] for pairs in data_raw.a]
                    symbol = data_raw.s

                    if next_full_fetch < time():
                        logger.log_msg(
                            f"Fetching {symbol} full market depth",
                            LoggingLevel.INFO,
                            symbol,
                        )
                        next_full_fetch += CONFIG.full_fetch_interval
                        loop.create_task(get_full_depth(symbol, session, database))

                    dispatcher.insert(
                        timestamp,
                        first_update_id,
                        final_update_id,
                        bids_quantity,
                        bids_price,
                        asks_quantity,
                        asks_price,
                        symbol,
                    )
                if msg.type == aiohttp.WSMsgType.CLOSE:
                    break
        logger.log_msg(
            f"Connection closed for {symbol} stream, retrying.",
            LoggingLevel.INFO,
            symbol,
        )
        next_full_fetch = time


async def setup():
    session = aiohttp.ClientSession()
    loop = asyncio.get_event_loop()
    database = Database(CONFIG.db_name)
    dispatcher = DiffDepthStreamDispatcher(database)
    logger = Logger(database)
    for symbol in CONFIG.symbols:
        loop.create_task(
            handle_depth_stream(symbol, session, dispatcher, database, logger, loop)
        )


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(setup())
    loop.run_forever()
