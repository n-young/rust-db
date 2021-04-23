#! /usr/bin/env sh

time cat $1 | cargo run client --release > /dev/null
