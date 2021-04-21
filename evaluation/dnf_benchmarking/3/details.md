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
    - real    0m39.148s
    - user    0m1.140s
    - sys     0m1.194s
- Queries (100):
    - real    0m11.981s
    - user    0m0.875s
    - sys     0m0.087s

Run 2 (packing only):
- Ingest (32400):
    - real    0m37.899s
    - user    0m1.212s
    - sys     0m1.027s
- Queries (100):
    - real    1m6.396s
    - user    0m0.093s
    - sys     0m0.034s

Run 3 (no dnf or packing):
- Ingest (32400):
    - real    0m35.378s
    - user    0m1.067s
    - sys     0m1.023s
- Queries (100):
    - real    2m5.817s
    - user    0m0.126s
    - sys     0m0.010s
