There are three relevant scripts

`load_small_data.sh` creates a workload. Tweak params to get what you like.

`generate_queries.py` and `generate_queries_naive.py` both generate 100 queries for a given workload. I recommend using the former.

`benchmark_file.sh` legit just prints out a file for you to pipe into `cargo run client`.
