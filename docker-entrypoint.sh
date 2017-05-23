#!/bin/bash
set -e

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 main.py $1
