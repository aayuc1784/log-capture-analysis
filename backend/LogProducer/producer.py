from kafka import KafkaProducer
import json
import argparse
import time

LOGS_TIME_INTERVAL=1
TEST_LOGS_PATH="logs"

LOG_LEVEL_TOPICS = {
    "auth",
    "database",
    "email",
    "payment",
    "server",
    "services"
}

parser = argparse.ArgumentParser(description='Kafka producer with user-specified topic')
parser.add_argument('--topic', required=True, choices=LOG_LEVEL_TOPICS, help='Kafka topic to produce logs to')
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=["localhost:9094"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic=args.topic

file_path = f"{TEST_LOGS_PATH}/{topic}.json"

print(f"Producing logs to topic - {topic} ...\n")
print(f"File path: {file_path}")


with open(file_path, "r") as file:
    data = json.load(file)
    for json_data in data:
        message = json.dumps(json_data)
        producer.send(topic, message)
        producer.flush()
        print(f"Produced ({topic}): {message}\n")
        time.sleep(LOGS_TIME_INTERVAL)

producer.close()