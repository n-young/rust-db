# Timeseries Database
Desmond Cheong + Nick Young

## Overview
This is a timeseries database written in Rust. It supports inserts and queries in JSON format. Project specification [here](https://www.francosolleza.com/CS227/systems-project.html).

## Design Decisions
- Only allowing key = label or metric = value; not the other way around
- Considering only allowing key=label AND metric=value; disallowing unbounded metrics?
