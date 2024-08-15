import report_pb2
from kafka import KafkaConsumer
import threading

broker = 'localhost:9092'

def debug_consumer():

    consumer = KafkaConsumer(
        bootstrap_servers=[broker],
        group_id="debug",  # consumer group named "debug"
    )
    consumer.subscribe(["temperatures"])

    while True:
        batch = consumer.poll(1000)
        for tp, messages in batch.items():
            for msg in messages:
                try:
                    # Kafka object typically has topic, partition, offset, key, value
                    report = report_pb2.Report() # from gRPC call that we established in producer.py
                    report.ParseFromString(msg.value) # have to deserialize protobuf string since Kafka is wire format
                    result = {
                        "partition": msg.partition,
                        "key": msg.key.decode("utf-8"),
                        "date": report.date,
                        "degrees": report.degrees
                    }
                    print(result)
                except Exception as e:
                    print(f"Failed, error: {e}")
    consumer.close()

threading.Thread(target=debug_consumer).start()
