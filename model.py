from datetime import datetime
from typing import List
from infi.clickhouse_orm.models import Model
from infi.clickhouse_orm.fields import (
    ArrayField,
    DateTime64Field,
    StringField,
    UInt8Field,
    Decimal128Field,
    UInt64Field,
    Field,
)
from infi.clickhouse_orm.database import Database, DatabaseException
from infi.clickhouse_orm.engines import MergeTree
from infi.clickhouse_orm.funcs import F
from enum import IntEnum
from decimal import Decimal


class LoggingLevel(IntEnum):
    CRITICAL = 50
    ERROR = 40
    WARNING = 30
    INFO = 20
    DEBUG = 10
    NOTSET = 0


class LoggingMsg(Model):
    timestamp = DateTime64Field()
    msg = StringField()
    level = UInt8Field(codec="Delta(4), LZ4")
    payload = StringField(default="")

    engine = MergeTree("timestamp", order_by=("timestamp",))


class DepthSnapshot(Model):
    timestamp = DateTime64Field()
    last_update_id = UInt64Field()
    bids_quantity = ArrayField(Decimal128Field(20))
    bids_price = ArrayField(Decimal128Field(20))
    asks_quantity = ArrayField(Decimal128Field(20))
    asks_price = ArrayField(Decimal128Field(20))
    symbol = StringField()

    engine = MergeTree(
        partition_key=(F.toYYYYMM(timestamp), "symbol"),
        order_by=("timestamp", "last_update_id"),
    )


class DiffDepthStream(Model):
    timestamp = DateTime64Field()
    first_update_id = UInt64Field()
    final_update_id = UInt64Field()
    bids_quantity = ArrayField(Decimal128Field(20))
    bids_price = ArrayField(Decimal128Field(20))
    asks_quantity = ArrayField(Decimal128Field(20))
    asks_price = ArrayField(Decimal128Field(20))
    symbol = StringField()

    engine = MergeTree(
        partition_key=(F.toYYYYMM(timestamp), "symbol"),
        order_by=("timestamp", "first_update_id", "final_update_id"),
    )


class DiffDepthStreamDispatcher:
    def __init__(self, database: Database, batch_size: int):
        self.buffer = []
        self.db = database
        self.batch_size = batch_size

    def insert(
        self,
        timestamp: datetime,
        first_update_id: int,
        final_update_id: int,
        bids_quantity: List[Decimal],
        bids_price: List[Decimal],
        asks_quantity: List[Decimal],
        asks_price: List[Decimal],
        symbol: str,
    ):
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

    def insert_to_db(self):
        try:
            self.db.insert(self.buffer)
            self.buffer = []
        except DatabaseException as e:
            print(e)
    
    def __len__(self):
        return len(self.buffer)

    def __repr__(self) -> str:
        return f"DiffDepthStreamDispatcher(len(buffer)={len(self.buffer)})"


if __name__ == "__main__":
    db = Database("archive")
    for model in [LoggingMsg, DepthSnapshot, DiffDepthStream]:
        db.create_table(model)
