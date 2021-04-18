#! /usr/bin/env sh

tsbs_generate_data \
    --churn=0.1 \
    --use-case="devops" \
    --seed=123 \
    --scale=2 \
    --timestamp-start="2016-01-01T00:00:00Z" \
    --timestamp-end="2016-01-04T00:00:00Z" \
    --log-interval="1m" \
    --format="influx" | ./influx_to_json.py | cargo run client

