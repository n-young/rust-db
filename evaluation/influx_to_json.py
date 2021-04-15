#! /usr/bin/env python3

import sys
import json
import pandas as pd

def influx_to_float(v):
    return float(v.rstrip("i"))

def parse_to_dict(kv_list, format_value=lambda x: x):
    d = {}
    for item in kv_list:
        item = item.split("=")
        d[item[0]] = format_value(item[1])
    return d

def parse_labels(labels):
    labels = labels.split(",")
    name = labels[0]
    label_dict = parse_to_dict(labels[1:])
    return name, label_dict

def parse_variables(variables):
    return parse_to_dict(variables.split(","), influx_to_float)

def influx_to_json(influx):
    sections = influx.split(" ")
    assert len(sections) == 3

    timestamp = str(pd.to_datetime(int(sections[2])).tz_localize("Utc"))
    name, labels = parse_labels(sections[0])
    variables = parse_variables(sections[1])

    record = {
            "name": name,
            "labels": labels,
            "variables": variables,
            "timestamp": timestamp
            }
    op = { "Write": record }

    return json.dumps(op)

def main(*args, **kwargs):
    for line in sys.stdin:
        line = line.rstrip()
        print(influx_to_json(line))

if __name__ == "__main__":
    main()
