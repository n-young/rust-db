# Notes
This was really way too big; I think it was because of the churn increase. I don't want to decesaes it, so I'm reducing the number of queries being evaluated instead.

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

Query generation scheme: targeted, minimal, 100 queries.

# Runs
Run 1 (dnf and packing):
- Ingest (32400):
    - real    1m34.048s
    - user    0m3.278s
    - sys     0m2.850s
- Queries (100):
    - real    3m14.971s
    - user    0m3.684s
    - sys     0m0.082s

Run 2 (packing only):
- Ingest (32400):
    - real    0m56.509s
    - user    0m1.823s
    - sys     0m1.643s
- Queries (100):
    - real    23m55.509s
    - user    0m3.078s
    - sys     0m0.040s

Run 3 (no dnf or packing):
- Ingest (32400):
    - Too long
- Queries (100):
    - Too long
