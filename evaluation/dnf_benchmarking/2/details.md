# Notes
Same workload as 1. Same queries as 1 (prime form). Fixed metric key bug. Fixed union and intersect bugs.


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
    - real    0m41.169s
    - user    0m1.233s
    - sys     0m1.225s
- Queries (100):
    - real    0m9.605s
    - user    0m0.707s
    - sys     0m0.098s

Run 2 (packing only):
- Ingest (32400):
    - real    0m38.743s
    - user    0m1.249s
    - sys     0m1.084s
- Queries (100):
    - real    1m0.860s
    - user    0m0.082s
    - sys     0m0.046s

Run 3 (no dnf or packing):
- Ingest (32400):
    - real    0m39.960s
    - user    0m1.312s
    - sys     0m1.089s
- Queries (100):
    - real    1m47.652s
    - user    0m0.119s
    - sys     0m0.011s