# Notes
CNF Generation

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

Query generation scheme: targeted, cnf, 10 queries.

# Runs
# Runs
Run 1 (dnf and packing):
- Ingest (32400):
    - real    1m23.338s
    - user    0m3.160s
    - sys     0m2.183s
- Queries (10):
    - real    0m1.202s
    - user    0m0.096s
    - sys     0m0.021s


Run 2 (packing only):
- Ingest (32400):
    - real    1m37.556s
    - user    0m3.439s
    - sys     0m2.892s
- Queries (10):
    - real    5m8.373s
    - user    0m0.085s
    - sys     0m0.032s


Run 3 (no dnf or packing):
- Ingest (32400):
    - real    1m33.024s
    - user    0m3.318s
    - sys     0m2.680s
- Queries (10):
    - real    4m56.741s
    - user    0m0.118s
    - sys     0m0.000s