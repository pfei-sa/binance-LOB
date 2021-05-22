from datetime import datetime
from typing import List, Optional
from infi.clickhouse_orm.models import Model
from infi.clickhouse_orm.fields import (
    ArrayField,
    DateTime64Field,
    StringField,
    UInt8Field,
    Float64Field,
    UInt64Field,
    LowCardinalityField,
)
from infi.clickhouse_orm.database import Database, DatabaseException
from infi.clickhouse_orm.engines import MergeTree, ReplacingMergeTree
from infi.clickhouse_orm.funcs import F
from enum import IntEnum
from config import CONFIG
import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%m/%d/%Y %I:%M:%S %p",
    level=logging.INFO,
)


class LoggingLevel(IntEnum):
    CRITICAL = 50
    ERROR = 40
    WARNING = 30
    INFO = 20
    DEBUG = 10
    NOTSET = 0


class Logger:
    def __init__(self, database: Database):
        self.db = database

    def log_msg(
        self,
        msg: str,
        level: LoggingLevel,
        payload: str = "",
        silence: Optional[bool] = None,
    ) -> None:
        silence = silence if silence is not None else CONFIG.log_to_console
        if silence:
            logging.log(level, msg)
        self.db.insert(
            [
                LoggingMsg(
                    timestamp=datetime.utcnow(), msg=msg, level=level, payload=payload
                )
            ]
        )


class LoggingMsg(Model):
    timestamp = DateTime64Field(codec="Delta,ZSTD")
    msg = StringField()
    level = UInt8Field(codec="Delta, LZ4")
    payload = StringField(default="")

    engine = MergeTree("timestamp", order_by=("timestamp",))


class DepthSnapshot(Model):
    timestamp = DateTime64Field(codec="Delta,ZSTD")
    last_update_id = UInt64Field()
    bids_quantity = ArrayField(Float64Field())
    bids_price = ArrayField(Float64Field())
    asks_quantity = ArrayField(Float64Field())
    asks_price = ArrayField(Float64Field())
    symbol = LowCardinalityField(StringField())

    engine = MergeTree(
        partition_key=("symbol",),
        order_by=("timestamp", "last_update_id"),
    )


class DiffDepthStream(Model):
    timestamp = DateTime64Field(codec="Delta,ZSTD")
    first_update_id = UInt64Field(codec="Delta,ZSTD")
    final_update_id = UInt64Field(codec="Delta,ZSTD")
    bids_quantity = ArrayField(Float64Field())
    bids_price = ArrayField(Float64Field())
    asks_quantity = ArrayField(Float64Field())
    asks_price = ArrayField(Float64Field())
    symbol = LowCardinalityField(StringField())

    engine = ReplacingMergeTree(
        partition_key=(F.toMonday(timestamp), "symbol"),
        order_by=("timestamp", "first_update_id", "final_update_id"),
    )


class DiffDepthStreamDispatcher:
    def __init__(self, database: Database, logger: Logger):
        self.buffer = []
        self.db = database
        self.batch_size = CONFIG.dispatcher_buffer_size
        self.logger = logger

    def insert(
        self,
        timestamp: datetime,
        first_update_id: int,
        final_update_id: int,
        bids_quantity: List[float],
        bids_price: List[float],
        asks_quantity: List[float],
        asks_price: List[float],
        symbol: str,
    ) -> None:
        self.buffer.append(
            DiffDepthStream(
                timestamp=timestamp,
                first_update_id=first_update_id,
                final_update_id=final_update_id,
                bids_quantity=bids_quantity,
                bids_price=bids_price,
                asks_quantity=asks_quantity,
                asks_price=asks_price,
                symbol=symbol,
            )
        )
        if len(self.buffer) >= self.batch_size:
            self.insert_to_db()

    def insert_to_db(self) -> None:
        try:
            self.db.insert(self.buffer)
            self.buffer = []
        except DatabaseException as e:
            self.logger.log_msg(
                f"{self.__repr__()} error, retrying:", LoggingLevel.WARNING, repr(e)
            )

    def __len__(self) -> int:
        return len(self.buffer)

    def __repr__(self) -> str:
        return f"DiffDepthStreamDispatcher(len(buffer)={len(self.buffer)})"


if __name__ == "__main__":
    db = Database(CONFIG.db_name, db_url=f"http://{CONFIG.host_name}:8123/")
    for model in [LoggingMsg, DepthSnapshot, DiffDepthStream]:
        db.create_table(model)
