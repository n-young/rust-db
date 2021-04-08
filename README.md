# Timeseries Database
Desmond Cheong + Nick Young

## Overview
This is a timeseries database written in Rust. It supports inserts and queries in JSON format. Project specification [here](https://www.francosolleza.com/CS227/systems-project.html).

## Getting Started
- Populate a `.env` file with the path of the data folder. We've suggested a default in the `sample.env` file.

## Design Decisions
- Only allowing key = label or metric = value; not the other way around
- Considering only allowing key=label AND metric=value; disallowing unbounded metrics?

## TODO
- Try using FSTs
- Get rid of the jank index serialization.
