# Notes
Pretty good, actually going to use this run.

# Generation
Workload generation command:
tsbs_generate_data \
    --churn=0.01 \
    --use-case="devops" \
    --seed=123 \
    --scale=1 \
    --timestamp-start="2016-01-01T00:00:00Z" \
    --timestamp-end="2016-01-01T01:00:00Z" \
    --log-interval="1s" \
    --format="influx" | ./influx_to_json.py

Query generation scheme: targeted, minimal, 20 queries.

Running in release profile.

# Runs
Run 1 (dnf and packing):
- Ingest (32400):
    - real    1m18.622s
    - user    0m2.955s
    - sys     0m2.093s
- Queries (20):
    - real    0m40.056s
    - user    0m0.712s
    - sys     0m0.042s

Run 2 (packing only):
- Ingest (32400):
    - real    1m30.694s
    - user    0m3.192s
    - sys     0m2.682s
- Queries (20):
    - real    4m52.767s
    - user    0m0.722s
    - sys     0m0.020

Run 3 (no dnf or packing):
- Ingest (32400):
    - real    1m14.290s
    - user    0m2.416s
    - sys     0m2.264s
- Queries (20):
    - real    5m0.669s
    - user    0m0.747s
    - sys     0m0.011s
