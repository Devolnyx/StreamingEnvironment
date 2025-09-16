from kafka import KafkaProducer
from faker import Faker
import numpy as np
import json
from time import sleep


topic_name = 'FirstTopic'
producer = KafkaProducer(bootstrap_servers='localhost:9092')
_instance = Faker()

def produce():
    while True:
        _data = {
            "first_name": _instance.first_name(),
            "city":_instance.city(),
            "phone_number":_instance.phone_number(),
            "state":_instance.state(),
            "id":str('_')
        }
        _payload = json.dumps(_data).encode("utf-8")
        response = producer.send(topic_name, _payload)
        #print(response)

        wait = np.random.randint(3,15,1)[0]
        sleep(wait)

if __name__ == "__main__":
    produce()

