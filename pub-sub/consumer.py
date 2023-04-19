
from kafka import KafkaConsumer
import json


## consuming the data with 1 partition and 1 consumer
# if __name__=="__main__":
#     consumer = KafkaConsumer(
#         "new_data",
#         bootstrap_servers='192.168.1.7:9092',
#         auto_offset_reset='earliest',
#         group_id="consumer-group-a")
    
#     print("starting the consumer")
#     for msg in consumer:
#         print("User registerd = ",json.loads(msg.value))


## consuming the data with 2 partition and 1 consumer
# if __name__=="__main__":
#     consumer = KafkaConsumer(
#         "users",
#         bootstrap_servers='192.168.1.7:9092',
#         auto_offset_reset='earliest',
#         group_id="consumer-group-a")
    
#     print("starting the consumer")
#     for msg in consumer:
#         print("User registerd = ",json.loads(msg.value))


## consuming the data with 1 partition and 2 consumers
# if __name__=="__main__":
#     consumer1 = KafkaConsumer(
#         "registered_user",
#         bootstrap_servers='192.168.1.7:9092',
#         auto_offset_reset='earliest',
#         group_id="consumer-group-a")
#     consumer2 = KafkaConsumer(
#         "registered_user",
#         bootstrap_servers='192.168.1.7:9092',
#         auto_offset_reset='earliest',
#         group_id="consumer-group-b")
    
#     print("starting the consumer\n")
#     for msg1,msg2 in zip(consumer1, consumer2):
#         print("User registerd 1 = ",json.loads(msg1.value))
#         print( "User registerd 2 = ",json.loads(msg2.value))
#         print()

## consuming the data with 2 partitions and 2 consumers
if __name__=="__main__":
    consumer = KafkaConsumer(
        "users",
        bootstrap_servers='192.168.1.7:9092',
        auto_offset_reset='earliest',
        group_id="consumer-group-a")
    
    print("starting the consumer\n")
    for msg in consumer:
        print("User registerd 1 = ",json.loads(msg.value))
        print()
