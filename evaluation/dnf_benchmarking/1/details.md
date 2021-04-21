# Notes
Run 1 was a bit of a failure - the query generation scheme made it such that we basically never got results. Used prime form queries.

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

Query generation scheme: naive

# Runs
Run 1 (dnf and packing):
- Ingest (32400):
    - real    1m0.112s
    - user    0m2.092s
    - sys     0m1.831s
- Queries (100):
    - real    0m4.760s
    - user    0m0.088s
    - sys     0m0.022s

Run 2 (packing only):
- Ingest (32400):
    - real    0m53.529s
    - user    0m1.752s
    - sys     0m1.669s
- Queries (100):
    - real    0m52.473s
    - user    0m0.089s
    - sys     0m0.020s

Run 3 (no dnf or packing):
- Ingest (32400):
    - real    0m57.098s
    - user    0m1.845s
    - sys     0m1.838s
- Queries (100):
    - real    1m33.912s
    - user    0m0.080s
    - sys     0m0.036s
