import time
import argparse

from kafka import KafkaProducer


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("-t", "--topic", type=str, dest="topic", help="Kafka topic to produce")
    parser.add_argument("-b", "--brokers", type=str, dest="brokers", help="Kafka brokers in format host:port")

    args = parser.parse_args()

    producer = KafkaProducer(bootstrap_servers=args.brokers)

    while True:
        sflow_line = input()
        if "CNTR" not in sflow_line:
            timestamp = int(time.time())
            producer.send(args.topic, str.encode("{0},{1}".format(timestamp, sflow_line)))
