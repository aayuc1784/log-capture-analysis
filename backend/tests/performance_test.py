import requests
import time
import random
from sample_logs import sample_logs
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from utils import generateCommit, generateRandomTimestamp

# Edit this to change the number of logs to be flooded to the ingestion server
LOGS_LENGTH = 300
PORT = 3000

BASE_URL = f"http://localhost:{PORT}"

# Repeat the sample_logs array as needed to reach or exceed the desired length n
logs = (sample_logs * (LOGS_LENGTH//len(sample_logs)+1))[:LOGS_LENGTH]
random.shuffle(logs)


start_date = datetime(2021, 1, 1)
end_date = datetime(2023, 12, 31)

for log in logs:
    log['timestamp'] = generateRandomTimestamp(start_date, end_date)
    log['commit'] = generateCommit()

print("Logs: ", len(logs))

def getLogsCount():
    response = requests.get(f"{BASE_URL}/logs-count")
    if response.status_code == 200:
        return response.json().get("count",0)
    else:
        print(f"Error getting logs count: {response.text}")

def postLogs(logs):
    response = requests.post(f"{BASE_URL}/", json=logs)
    if response.status_code != 202:
        print(f"Error in sending logs:{response.text}")

def scalabilityTest():
    with ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(postLogs, logs)

if __name__ == "__main__":
    initial_logs_count = getLogsCount()

    print(f"Number of logs in the database before ingestion:{initial_logs_count}")

    print("Start ingesting the logs.....")

    scalabilityTest()

    print(f"Ingested {LOGS_LENGTH} logs to the log server")

    time.sleep(2)

    post_logs_count = getLogsCount()

    print(f"Number of logs in the database right now: {post_logs_count}")

    print(f"Rest, are in the buffer waiting to be ingested !!")