from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError
from kafka.errors import TopicAlreadyExistsError
from report_pb2 import Report
import weather
import datetime
import threading
import time

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3) # Deletion sometimes takes a while to reflect

try:
    # if we had JSON instead of protobufs we would have "temperatures-json"
    admin_client.create_topics([NewTopic(name="temperatures", num_partitions=4, replication_factor=1)])
except TopicAlreadyExistsError:
    print("already exists")

# # Runs infinitely because the weather never ends
# for date, degrees in weather.get_next_weather(delay_sec=0.1):
#     print(date, degrees) # date, max_temperature


def producer():
    # acks all: after data is committed, slowest but strongest, retries: 10
    # acks="all" and retries=10 prevent undercounting
    producer = KafkaProducer(
        bootstrap_servers=[broker], retries=10, acks="all"  
    )
    weather_generator = weather.get_next_weather()
    for date, degrees in weather_generator:
        report = Report(date=date, degrees=degrees).SerializeToString()

        # the syntax of gettin the month name from the date from the weather_generator from weather.py
        month_name = datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%B")
        
        # we are serializing the message value, which the debug.py will deserialize for wire format
        producer.send(
            "temperatures",
            key=bytes(month_name, "utf-8"),
            value=report
        )
        #print(f"Sent data for {date}: {degrees} degrees")

threading.Thread(target=producer).start()

print("Topics:", admin_client.list_topics())
