#!/usr/bin/env python
import time
import json
import paho.mqtt.client as paho
import warnings
import random

warnings.filterwarnings("ignore")

user = 'proj7'
pwd = 'integrador@'
broker = "142.47.103.158"
topic = 'PI7'
message = ''

client = paho.Client() 
print("Connecting to broker ",broker)
client.username_pw_set(user, password=pwd)
client.connect(broker, 1883)

while True:
	valor = random.randint(0,999)

	print("Publishing...", json.dumps({"id": "miguel-db", "data": "%s" %valor}))
	client.publish(topic, json.dumps({"id": "miguel-db", "data": "%s" %valor}))
	time.sleep(3)
