from typing import List

from dagster import (
    Nothing, 
    String, 
    asset, 
    with_resources
)
from workspaces.resources import redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@asset(
    group_name="corise",
    required_resource_keys={"s3"},
    config_schema={"s3_key": String},
)
def get_s3_data(context):
    s3_client = context.resources.s3
    return [Stock.from_list(stock) for stock in s3_client.get_data(context.op_config["s3_key"])]


@asset(
    group_name="corise",
)
def process_data(stock_list):
    highest_stock = max(stock_list, key=lambda x: x.high)
    return Aggregation(date=highest_stock.date, high=highest_stock.high)

@asset(
    group_name="corise",
    required_resource_keys={"redis"},
)
def put_redis_data(context, aggregation):
    redis_client = context.resources.redis
    redis_client.put_data("stock_aggregation", aggregation.date.strftime("%Y/%m/%d") + ', ' + str(aggregation.high))


@asset(
    group_name="corise",
    required_resource_keys={"s3"},
)
def put_s3_data(context, aggregation):
    s3_client = context.resources.s3
    s3_client.put_data("stock_aggregation", aggregation)


get_s3_data_docker, process_data_docker, put_redis_data_docker, put_s3_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data, put_s3_data],
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    resource_config_by_key={
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
    }
)
