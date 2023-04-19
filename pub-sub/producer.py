
from kafka import KafkaProducer
from data import get_registered_user
from time import sleep
import json


## sending data to single partition
# def json_serializer(data):
#     return json.dumps(data).encode("utf-8")
# producer = KafkaProducer(bootstrap_servers=['192.168.1.7:9092'], 
#                          value_serializer=json_serializer)
# if __name__=="__main__":
#     while 1:
#         registerd_user = get_registered_user()
#         print(registerd_user)
#         producer.send("registered_user", registerd_user)
#         sleep(5)
        

## sending data to 2 partitions
def json_serializer(data):
    return json.dumps(data).encode("utf-8")
producer = KafkaProducer(bootstrap_servers=['192.168.1.7:9092'], 
                         value_serializer=json_serializer)
if __name__=="__main__":
    while 1:
        registerd_user = get_registered_user()
        print(registerd_user)
        producer.send("users", registerd_user)
        sleep(5)

## sending data to perticular partition
# def get_partition(key, all, available):
#     return 0
# producer = KafkaProducer(bootstrap_servers=['192.168.1.7:9092'], 
#                          value_serializer=json_serializer,
#                          partitioner=get_partition)
# if __name__=="__main__":
#     while 1:
#         registerd_user = get_registered_user()
#         print(registerd_user)
#         producer.send("temporary_users", registerd_user)
#         sleep(5)

