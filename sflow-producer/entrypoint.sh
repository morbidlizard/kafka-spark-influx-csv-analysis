#!/bin/bash
set -e

kafka-topics --create --topic $TOPIC --partitions 1 --replication-factor 1 --if-not-exists --zookeeper $ZK
kafka-topics --describe --topic $TOPIC --zookeeper $ZK

sflowtool -l | python3 producer.py -t $TOPIC -b $BROKER
