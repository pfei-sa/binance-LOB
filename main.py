from asyncio.events import AbstractEventLoop, get_event_loop
from typing import List
from aiohttp.client import ClientSession
from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.engines import Log
from model import DiffDepthStreamDispatcher, LoggingMsg, LoggingLevel
from datetime import datetime
import asyncio
import aiohttp
from pydantic import BaseModel, main
from decimal import Decimal
import json
from time import time


class DiffDepthStreamMsg(BaseModel):
    e: str  # Event type
    E: int  # Event time (Unix Epoch ms)
    s: str  # Symbol
    U: int  # start update id
    u: int  # end update id
    b: List[List[Decimal]]  # bids [price, quantity]
    a: List[List[Decimal]]  # asks [price, quantity]


def depth_stream_url(symbol: str, speed: int = 1000) -> str:
    assert speed in (1000, 100), "speed must be 1000 or 100"
    endpoint = f"{symbol}@depth" if speed == 1000 else f"{symbol}@depth@100ms"
    return f"wss://stream.binance.com:9443/ws/{endpoint}"


async def get_full_depth(symbol: str, session: ClientSession):
    pass


async def handle_depth_stream(
    symbol: str,
    session: ClientSession,
    dispatcher: DiffDepthStreamDispatcher,
    database: Database,
    loop: AbstractEventLoop,
    speed: int = 1000,
    full_fetch_interval: int = 60*60
):
    symbol = symbol.lower()
    next_full_fetch = time()
    while True:
        async with session.ws_connect(depth_stream_url(symbol, speed)) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data_raw = DiffDepthStreamMsg(**msg.json())
                    s = data_raw.E / 1000.0
                    timestamp = datetime.fromtimestamp(s)
                    first_update_id = data_raw.U
                    final_update_id = data_raw.U
                    bids_quantity = [pairs[1] for pairs in data_raw.b]
                    bids_price = [pairs[0] for pairs in data_raw.b]
                    asks_quantity = [pairs[1] for pairs in data_raw.a]
                    asks_price = [pairs[0] for pairs in data_raw.a]
                    symbol = data_raw.s

                    if next_full_fetch < time():
                        next_full_fetch += full_fetch_interval
                        loop.create_task(get_full_depth(symbol))

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


async def setup():
    session = aiohttp.ClientSession()
    loop = asyncio.get_event_loop()
    database = Database("archive")
    dispatcher = DiffDepthStreamDispatcher(database, 100)
    loop.create_task(handle_depth_stream("btcusdt", session, dispatcher, database, loop, 100))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(setup())
    loop.run_forever()
