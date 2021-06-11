from asyncio.events import AbstractEventLoop
from typing import List, Optional
from aiohttp.client import ClientSession
from infi.clickhouse_orm.database import Database
from model import (
    DepthSnapshot,
    DiffDepthStreamDispatcher,
    Logger,
    LoggingLevel,
    DiffDepthStream,
    LoggingMsg,
)
from datetime import datetime
import asyncio
import aiohttp
from pydantic import BaseModel, ValidationError
from time import time
from config import CONFIG
from enum import Enum


class AssetType(Enum):
    SPOT = ""
    USD_M = "USD_F_"
    COIN_M = "COIN_F_"


class DiffDepthStreamMsg(BaseModel):
    e: str  # Event type
    E: int  # Event time (Unix Epoch ms)
    s: str  # Symbol
    U: int  # start update id
    u: int  # end update id
    b: List[List[float]]  # bids [price, quantity]
    a: List[List[float]]  # asks [price, quantity]
    pu: Optional[int]


class DepthSnapshotMsg(BaseModel):
    lastUpdateId: int
    bids: List[List[float]]
    asks: List[List[float]]
    E: Optional[int]
    T: Optional[int]


def depth_stream_url(symbol: str, asset_type: AssetType) -> str:
    speed = CONFIG.stream_interval
    assert speed in (1000, 100), "speed must be 1000 or 100"
    symbol = symbol.lower()
    endpoint = f"{symbol}@depth" if speed == 1000 else f"{symbol}@depth@100ms"
    if asset_type == AssetType.SPOT:
        return f"wss://stream.binance.com:9443/ws/{endpoint}"
    elif asset_type == AssetType.USD_M:
        return f"wss://fstream.binance.com/ws/{endpoint}"
    else:
        return f"wss://dstream.binance.com/ws/{endpoint}"


async def get_full_depth(
    symbol: str, session: ClientSession, database: Database, asset_type: AssetType
):
    limit = CONFIG.full_fetch_limit
    if asset_type == AssetType.SPOT:
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit={limit}"
    elif asset_type == AssetType.USD_M:
        url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit={limit}"
    elif asset_type == AssetType.COIN_M:
        url = f"https://dapi.binance.com/dapi/v1/depth?symbol={symbol}&limit={limit}"
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
            symbol=asset_type.value + symbol,
        )
        database.insert([snapshot])


async def handle_depth_stream(
    symbol: str,
    session: ClientSession,
    dispatcher: DiffDepthStreamDispatcher,
    database: Database,
    logger: Logger,
    loop: AbstractEventLoop,
    asset_type: AssetType,
):
    next_full_fetch = time()
    logger.log_msg(
        f"Connecting to {asset_type.value + symbol} stream", LoggingLevel.INFO, symbol
    )
    prev_final_update_id = None
    while True:
        async with session.ws_connect(depth_stream_url(symbol, asset_type)) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data_raw = DiffDepthStreamMsg(**msg.json())
                    except ValidationError:
                        print(msg.data)
                        break

                    s = data_raw.E / 1000.0
                    timestamp = datetime.utcfromtimestamp(s)
                    if asset_type == AssetType.SPOT:
                        first_update_id = data_raw.U
                        final_update_id = data_raw.u
                    else:
                        first_update_id = data_raw.pu + 1
                        final_update_id = data_raw.u
                    bids_quantity = [pairs[1] for pairs in data_raw.b]
                    bids_price = [pairs[0] for pairs in data_raw.b]
                    asks_quantity = [pairs[1] for pairs in data_raw.a]
                    asks_price = [pairs[0] for pairs in data_raw.a]
                    symbol = data_raw.s
                    symbol_full = asset_type.value + symbol

                    if next_full_fetch < time():
                        logger.log_msg(
                            f"Fetching {symbol_full} full market depth",
                            LoggingLevel.INFO,
                            symbol_full,
                        )
                        next_full_fetch += CONFIG.full_fetch_interval
                        loop.create_task(
                            get_full_depth(symbol, session, database, asset_type)
                        )
                    if (
                        prev_final_update_id
                        and prev_final_update_id + 1 != first_update_id
                    ):
                        logger.log_msg(
                            f"LOB dropped for {symbol_full}, refetching full market depth",
                            LoggingLevel.INFO,
                            symbol_full,
                        )

                    dispatcher.insert(
                        timestamp,
                        first_update_id,
                        final_update_id,
                        bids_quantity,
                        bids_price,
                        asks_quantity,
                        asks_price,
                        symbol_full,
                    )
                if msg.type == aiohttp.WSMsgType.CLOSE:
                    break
        logger.log_msg(
            f"Connection closed for {symbol_full} stream, retrying.",
            LoggingLevel.INFO,
            symbol,
        )
        next_full_fetch = time()


async def setup():
    session = aiohttp.ClientSession()
    loop = asyncio.get_event_loop()
    database = Database(CONFIG.db_name, db_url=f"http://{CONFIG.host_name}:8123/")
    logger = Logger(database)
    dispatcher = DiffDepthStreamDispatcher(database, logger)
    logger.log_msg("Starting event loop...", LoggingLevel.INFO)
    for symbol in CONFIG.symbols:
        if "USD_" in symbol:
            loop.create_task(
                handle_depth_stream(
                    symbol[4:],
                    session,
                    dispatcher,
                    database,
                    logger,
                    loop,
                    AssetType.USD_M,
                )
            )
        elif "COIN_" in symbol:
            loop.create_task(
                handle_depth_stream(
                    symbol[5:],
                    session,
                    dispatcher,
                    database,
                    logger,
                    loop,
                    AssetType.COIN_M,
                )
            )
        else:
            loop.create_task(
                handle_depth_stream(
                    symbol, session, dispatcher, database, logger, loop, AssetType.SPOT
                )
            )


if __name__ == "__main__":
    db = Database(CONFIG.db_name, db_url=f"http://{CONFIG.host_name}:8123/")
    for model in [LoggingMsg, DepthSnapshot, DiffDepthStream]:
        db.create_table(model)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(setup())
    loop.run_forever()
