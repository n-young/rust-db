#! /usr/bin/env python3

# ==================================
# Generates queries for a given
# workload given its workload.
# ==================================

import json
import math
import numpy as np
import random
random.seed(1)
import sys

# Define hyperparameters
NUM_QUERIES = 100 # Number of queries to generate
MAX_HEIGHT = 3 # Maximum height of the query tree.
FRAC_LABELS = 0.5 # Proportion of Leafs that are labels.
FRAC_AND = 0.5 # Proportion of branches that are Ands.
FRAC_VARIANCE = 0.5 # Proportion of each metric mean that we should be allowed to deviate from.
ops = ["Eq", "NEq", "Gt", "Lt", "GtEq", "LtEq"]

# Function to get label and metric metadata given a filename.
def get_metadata(filename):
    # Metadata ingest vars.
    label_sets = {}
    variables = {}

    # For each line in the workload...
    with open(filename, 'r') as file:
        for line in file:

            # Convert the line from JSON, get the labels and variables.
            obj = json.loads(line)
            obj_labels = obj['Write']['labels']
            obj_variables = obj['Write']['variables']

            # Extract labels as sets.
            for k in obj_labels:
                if k in label_sets:
                    label_sets[k].add(obj_labels[k])
                else:
                    label_sets[k] = {obj_labels[k]}

            # Extract variables as lists.
            for k in obj_variables:
                if k in variables:
                    variables[k].append(obj_variables[k])
                else:
                    variables[k] = [obj_variables[k]]

    # Processed metadata vars.
    labels = {}
    metrics = {}

    # Convert labels to lists.
    for k in label_sets:
        labels[k] = [x for x in label_sets[k]]

    # Normalize variables.
    for k in variables:
        metrics[k] = sum(variables[k]) / len(variables[k])

    # Return.
    return labels, metrics

# Function to generate a label pair.
def generate_label_pair(labels):
    labelkey = random.choice(list(labels.keys()))
    labelvalue = random.choice(labels[labelkey])
    return labelkey, labelvalue

# Function to generate a metric pair.
def generate_metric_pair(metrics):
    metricvariable = random.choice(list(metrics.keys()))
    metricmean = metrics[metricvariable]
    metricvariance = metricmean * FRAC_VARIANCE
    metricvalue = math.trunc(np.random.normal(loc=metricmean, scale=metricvariance) * 1000) / 1000
    return metricvariable, metricvalue

# Function to generate a condition recursively.
def generate_condition(height, labels, metrics):
    if height == 0: # Generate a Leaf
        if random.random() > FRAC_LABELS:
            labelkey, labelvalue = generate_label_pair(labels)
            ret = {
                "Leaf": {
                    "lhs": { "LabelKey": labelkey },
                    "rhs": { "LabelValue": labelvalue },
                    "op": "Eq"
                }
            }
        else:
            metricvariable, metricvalue = generate_metric_pair(metrics)
            ret = {
                "Leaf": {
                    "lhs": { "Variable": metricvariable },
                    "rhs": { "Metric": metricvalue },
                    "op": random.choice(ops)
                }
            }
        return ret

    else: # Generate an And or an Or, and recurse
        op = "And" if random.random() > FRAC_AND else "Or"
        ret = {
            op: [ generate_condition(height-1, labels, metrics), generate_condition(height-1, labels, metrics)]
        }
        return ret
        
        
# Generate NUM_QUERIES queries.
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: ./generate_queries <workload filename>")
        exit(0)
    
    filename = sys.argv[1]
    labels, metrics = get_metadata(filename)
    for _ in range(NUM_QUERIES):
        query = {
            "Select": {
                "name": "generated_select",
                "predicate": {
                    "name": "generated_predicate",
                    "condition": generate_condition(MAX_HEIGHT, labels, metrics)
                }
            }
        }
        print(json.dumps(query))


