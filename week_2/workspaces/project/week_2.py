from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    config_schema={"s3_key": String},
    out = {"stock_list": Out(dagster_type=List[Stock], description="stock data from s3")}, 
)
def get_s3_data(context):
    file_name = context.op_config["s3_key"]
    return [stock for stock in csv_helper(file_name)]


@op(
    ins = {"stock_list": In(dagster_type=List[Stock], description="valid stock data")},
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
    required_resource_keys={"database"},
    tags={"kind": "redis"},
)
def put_redis_data():
    pass


@op
def put_s3_data():
    pass


@graph
def week_2_pipeline():
    pass


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
)
