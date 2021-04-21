# Notes
Same workload as 1 and 2. Used new query generation scheme, still prime form.


# Generation
Workload generation command:
tsbs_generate_data \
    --churn=0.001 \
    --use-case="devops" \
    --seed=123 \
    --scale=1 \
    --timestamp-start="2016-01-01T00:00:00Z" \
    --timestamp-end="2016-01-01T01:00:00Z" \
    --log-interval="1s" \
    --format="influx" | ./influx_to_json.py

Query generation scheme: smarter

# Runs
Run 1 (dnf and packing):
- Ingest (32400):

- Queries (100):


Run 2 (packing only):
- Ingest (32400):
    - real    0m27.942s
    - user    0m0.875s
    - sys     0m0.754s
- Queries (100):
    - real    1m34.654s
    - user    0m0.163s
    - sys     0m0.038s

Run 3 (no dnf or packing):
- Ingest (32400):

- Queries (100):

