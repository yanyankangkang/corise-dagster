import numpy as np
from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    schedule,
    sensor,
    String,
    static_partitioned_config,
)
from workspaces.project.sensors import get_s3_keys
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock


@op(
    required_resource_keys={"s3"},
    config_schema={"s3_key": String},
    out = {"stock_list": Out(dagster_type=List[Stock], description="stock data from s3")}, 
)
def get_s3_data(context):
    s3_client = context.resources.s3
    return [Stock.from_list(stock) for stock in s3_client.get_data(context.op_config["s3_key"])]


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
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
    ins = {"aggregation": In(dagster_type=Aggregation, description="the date with highest stock price")},
)
def put_redis_data(context, aggregation):
    redis_client = context.resources.redis
    redis_client.put_data("stock_aggregation", aggregation.date.strftime("%Y/%m/%d") + ', ' + str(aggregation.high))


@op(
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
    ins = {"aggregation": In(dagster_type=Aggregation, description="the date with highest stock price")},
)
def put_s3_data(context, aggregation):
    s3_client = context.resources.s3
    s3_client.put_data("stock_aggregation", aggregation)


@graph
def week_3_pipeline():
    stock_list = get_s3_data()
    agg_res = process_data(stock_list)
    put_redis_data(agg_res)
    put_s3_data((agg_res))


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}

@static_partitioned_config(partition_keys=[str(c) for c in np.arange(1,11,1)])
def docker_config(month: String):
    return {
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
        "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_%s.csv" % month}}},
    }


week_3_pipeline_local = week_3_pipeline.to_job(
    name="week_3_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

week_3_pipeline_docker = week_3_pipeline.to_job(
    name="week_3_pipeline_docker",
    config=docker_config,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


week_3_schedule_local =  ScheduleDefinition(
    job=week_3_pipeline_local, cron_schedule="*/15 * * * *"
)


@schedule(
    job=week_3_pipeline_docker, cron_schedule="0 * * * *"
)
def week_3_schedule_docker():
    for month in np.arange(1, 11, 1):
        s3_key = "prefix/stock_%s.csv" % str(month)
        request = week_3_pipeline_docker.run_request_for_partition(partition_key=s3_key, run_key=s3_key)
        yield request

@sensor(
    job=week_3_pipeline_docker
)
def week_3_sensor_docker(context):
    s3_keys = get_s3_keys(bucket="bucket", prefix="prefix")
    if len(s3_keys) == 0:
        yield SkipReason("No new s3 files found in bucket.")
    else:
        for s3_key in s3_keys:
            docker["ops"]["get_s3_data"]["config"]["s3_key"] = s3_key
            yield RunRequest(run_key=s3_key, run_config=docker)