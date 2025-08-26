
from kafka import KafkaProducer, KafkaConsumer
from fastapi import FastAPI
from publisher.get_data import GetData
import json

app = FastAPI()
get_data = GetData()

# @app.get("/interesting")
# def get_interesting():
#     return get_data.get_json("data/newsgroups_interesting.json")

# @app.get("/not_interesting")
# def get_not_interesting():
#     return get_data.get_json("data/newsgroups_not_interesting.json")


def get_specific_data():

    my_dict = {"interesting": [], "not_interesting": []}
    news_interesting = get_data.get_json("data/newsgroups_interesting.json")
    news_not_interesting = get_data.get_json("data/newsgroups_not_interesting.json")

    for message in news_interesting:
        if not any(d.get("category") == message["category"] for d in my_dict["interesting"]):
            my_dict["interesting"].append({"category": message["category"], "text": message["text"]})

    for message in news_not_interesting:
        if not any(d.get("category") == message["category"] for d in my_dict["not_interesting"]):
            my_dict["not_interesting"].append({"category": message["category"], "text": message["text"]})

    return my_dict

def get_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],  # הכתובת של Kafka
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def publish_message(producer, topic, message):
    producer.send(topic, message)
    producer.flush()



producer = get_producer()
event = get_specific_data()
publish_message(producer, "topic_interesting", event["interesting"])
print("✅ Message sent to Kafka") 
publish_message(producer, "topic_not_interesting", event["not_interesting"])
print("✅ Message sent to Kafka") 