import sys
import json
import os
from kafka import KafkaConsumer, TopicPartition
from datetime import datetime
import threading
import report_pb2

broker = 'localhost:9092'

def load_partition_data(partition_num):
    path = f"files/partition-{partition_num}.json"
    if os.path.exists(path):
        with open(path, "r") as file:
            return json.load(file)
    else:
        return {"partition": partition_num, "offset": 0}

def save_partition_data(partition_num, loaded_data):

    # here we are doing the atomic writes, to write a new version
    path1 = f"files/partition-{partition_num}.json"
    path2 = f"{path1}.tmp"
    with open(path2, "w") as file:
        json.dump(loaded_data, file, indent=4)
        os.rename(path2, path1)

def provide_statistics(partition_output, report):

    # this is invalid, so we skip this
    if report.degrees >= 100000:
        return
    
    date = datetime.strptime(report.date, "%Y-%m-%d")
    month = date.strftime("%B")
    year = date.year
    if month not in partition_output:
        partition_output[month] = {}
    if year not in partition_output[month]:
        partition_output[month][year] = {"count": 0, 
                             "sum": 0.0, 
                             "avg": 0, 
                             "start": report.date, 
                             "end": report.date}

    partition_output[month][year]["count"] += 1
    partition_output[month][year]["sum"] += float(report.degrees)
    partition_output[month][year]["avg"] = (float(partition_output[month][year]["sum"] 
        / partition_output[month][year]["count"]))
    partition_output[month][year]["end"] = report.date  

def consumer(partition_input_list):
    partition_output = {}
    consumer = KafkaConsumer(bootstrap_servers=broker)
    consumer.assign([TopicPartition("temperatures", p) for p in partition_input_list])
    # we are loading/reading the JSON files here
    for p in partition_input_list:
        partition_output[p] = load_partition_data(p)
        consumer.seek(TopicPartition("temperatures", p), partition_output[p]["offset"])

    while True:
        batch = consumer.poll(1000)
        for tp, messages in batch.items():
            for msg in messages:
                report = report_pb2.Report()
                report.ParseFromString(msg.value) # have to deserialize protobuf string since Kafka is wire format
                provide_statistics(partition_output[tp.partition], report)

                #record the current read offset in the file
                partition_output[tp.partition]["offset"] = consumer.position(tp)
            save_partition_data(tp.partition, partition_output[tp.partition])
    consumer.close()
threading.Thread(target=consumer, args=([int(p) for p in sys.argv[1:]],)).start()
