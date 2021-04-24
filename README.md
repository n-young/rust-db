# TRustDB
[![Build & Tests](https://github.com/n-young/trustdb/actions/workflows/rust.yml/badge.svg)](https://github.com/n-young/trustdb/actions/workflows/rust.yml)

Welcome to TRustDB (_Time series Rust DataBase_), a database optimized for handling high-cardinality time series data. The technical report can be found [here](TRustDB.pdf).

By Desmond Cheong & Nick Young

## Getting Started
- Populate a `.env` file with the path of the data folder. We've suggested a default in the `sample.env` file.

## Key Design Choices
- To support efficient queries, an inverted index is constructed that maps from label key-value pairs and metric names to series.
- To reduce the storage footprint of this metadata, the inverted index is compressed into a Finite State Transducer (FST).
- To speed up query evaluation, queries are rewritten and evaluated in Disjunctive Normal Form (DNF).

More details can be found in the [technical report](TRustDB.pdf).

## Roadmap & Contributing
This database is far from complete and we have a lot more ideas to experiment with. Some of the things on our mind can be found on the [issues page](https://github.com/n-young/trustdb/issues).

Contributions are welcome!
