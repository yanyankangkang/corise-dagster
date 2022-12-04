
from typing import List, Iterator
from dagster import In, Nothing, Out, ResourceDefinition, String, graph, op
from workspaces.resources import mock_s3_resource, redis_resource, s3_resource
from workspaces.types import Aggregation, Stock

def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)
@op(
    required_resource_keys={"s3"},
    config_schema={"s3_key": String},
    out = {"stock_list": Out(dagster_type=List[Stock], description="stock data from s3")}, 
)
def get_s3_data(context):
    # file_name = os.path.join("week_2", context.op_config["s3_key"])
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
def week_2_pipeline():
    stock_list = get_s3_data()
    agg_res = process_data(stock_list)
    put_redis_data(agg_res)
    put_s3_data((agg_res))

local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "data/stock.csv"}}},
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
    "ops": {"get_s3_data": {"config": {"s3_key": "data/stock.csv"}}},
}

week_2_pipeline_local = week_2_pipeline.to_job(
    name="week_2_pipeline_local",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

week_2_pipeline_docker = week_2_pipeline.to_job(
    name="week_2_pipeline_docker",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
