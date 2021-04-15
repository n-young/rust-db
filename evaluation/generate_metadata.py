import json

# Workload file.
FILENAME = "./workloads/workload_128k.txt"

# Ingest vars
label_sets = {}
variables = {}

# For each line in the above file...
with open(FILENAME, 'r') as file:
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

# Processing vars
labels = {}
metrics = {}

# Convert labels to lists
for k in label_sets:
    labels[k] = [x for x in label_sets[k]]

# Normalize variables
for k in variables:
    metrics[k] = sum(variables[k]) / len(variables[k])

# Print out
print(json.dumps(labels))
print(json.dumps(metrics))
