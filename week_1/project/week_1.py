import csv
from datetime import datetime
from typing import Iterator, List
from dagster import In, DagsterType, Nothing, Out, String, job, op, usable_as_dagster_type
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[List]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )

def stock_list_checker(key_name, stock_list: List[Stock]):
    if len(stock_list) == 0:
        return False
    for stock in stock_list:
        if not hasattr(stock, 'date') or not isinstance(stock.date, datetime) or \
            not hasattr(stock, 'high')  or not isinstance(stock.high, float):
            return False
    return True

ProcessType = DagsterType(
    type_check_fn=stock_list_checker, name="process_op_checker", description="Every stock object that must include valid date and high price"
)


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(
    config_schema={"s3_key": String},
    out = {"stock_list": Out(dagster_type=List[Stock], description="stock data from s3")}, 
)
def get_s3_data(context):
    file_name = context.op_config["s3_key"]
    return [stock for stock in csv_helper(file_name)]


@op(
    ins = {"stock_list": In(dagster_type=ProcessType, description="valid stock data")},
    out = {"aggregation": Out(dagster_type=Aggregation, description="the date with highest stock price")}
)
def process_data(context, stock_list):
    highest = float('-inf')
    timestamp = None
    for stock in stock_list:
        if highest < stock.high:
            highest = stock.high
            timestamp = stock.date
    return Aggregation(date=timestamp, high=highest)

@op(
    ins = {"aggregation": In(dagster_type=Aggregation, description="the date with highest stock price")},
)
def put_redis_data(context, aggregation):
    pass


@job
def week_1_pipeline():
    stock_list = get_s3_data()
    agg_res = process_data(stock_list)
    put_redis_data(agg_res)