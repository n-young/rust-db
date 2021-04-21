#! /usr/bin/env python3

import json
import math
import numpy as np
import random
random.seed(1)
import sys

FRAC_VARIANCE = 0.5 # Proportion of each metric mean that we should be allowed to deviate from.
ops = ["Gt", "Lt", "GtEq", "LtEq"]

def get_key(labels, metrics):
    key = ""
    label_tuples = [(x, labels[x]) for x in labels]
    label_tuples.sort(key=lambda x: x[0])
    metric_tuples = [(x, metrics[x]) for x in metrics]
    metric_tuples.sort(key=lambda x: x[0])
    for x in label_tuples:
        key += x[0] + x[1]
    for x in metric_tuples:
        key += x[0]
    return key
    

def get_series(filename):
    # Metadata ingest vars.
    series = {}
    
     # For each line in the workload...
    with open(filename, 'r') as file:
        for line in file:
            # Convert the line from JSON, get the labels and variables.
            obj = json.loads(line)
            obj_labels = obj['Write']['labels']
            obj_metrics = obj['Write']['variables']
            obj_key = get_key(obj_labels, obj_metrics)
            if obj_key in series:
                series[obj_key].append((obj_labels, obj_metrics))
            else:
                series[obj_key] = [(obj_labels, obj_metrics)]

    return series

def generate_label_pair(labels):
    labelkey = random.choice(list(labels.keys()))
    labelvalue = labels[labelkey]
    return labelkey, labelvalue

# Function to generate a metric pair.
def generate_metric_pair(metrics):
    metricvariable = random.choice(list(metrics.keys()))
    metricvalue = metrics[metricvariable]
    return metricvariable, metricvalue

def generate_label_leaf(labels):
    labelkey, labelvalue = generate_label_pair(labels)
    ret = {
        "Leaf": {
            "lhs": { "LabelKey": labelkey },
            "rhs": { "LabelValue": labelvalue },
            "op": "Eq"
        }
    }
    return ret

def generate_metric_leaf(metrics):
    metricvariable, metricvalue = generate_metric_pair(metrics)
    ret = {
        "Leaf": {
            "lhs": { "Variable": metricvariable },
            "rhs": { "Metric": metricvalue },
            "op": random.choice(ops)
        }
    }
    return ret

def generate_min_condition(series):
    _, series_choices = random.choice(list(series.items()))
    labels, metrics = series_choices[0]
    ret = {
        "And": [
            {
                "Or": [
                    {
                        "And": [
                            generate_label_leaf(labels),
                            generate_metric_leaf(metrics)
                        ]
                    },
                    generate_metric_leaf(metrics)
                ]
            },
            generate_label_leaf(labels)
        ]
    }
    return ret

# Generate NUM_QUERIES queries.
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ./generate_queries <workload filename>")
        exit(0)
    
    filename = sys.argv[1]
    series = get_series(filename)

    for _ in range(100):
        query = {
            "Select": {
                "name": "generated_select",
                "predicate": {
                    "name": "generated_predicate",
                    "condition": generate_min_condition(series)
                }
            }
        }
        print(json.dumps(query))

