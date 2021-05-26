#!/usr/bin/env python
from pyowm import OWM
from pyowm.utils import config
from pyowm.utils import timestamps
import time
import json
import paho.mqtt.client as paho
import warnings
import random

warnings.filterwarnings("ignore")

user = '--'
pwd = '--'
broker = "--"
topic = 'PI7'

client = paho.Client() 
print("Connecting to broker ",broker)
client.username_pw_set(user, password=pwd)
client.connect(broker, 1883)

# API Secret Key
owm = OWM('--')
mgr = owm.weather_manager()

while True:
    observation = mgr.weather_at_place('Pelotas,BR')
    w = observation.weather
    
    temp = {"temp": w.temperature('celsius')['temp'], "date": observation.reception_time(timeformat='iso')}
    
    # temp = w.temperature('celsius')['temp']
    obj = json.dumps({"id": "miguel-db-temp", "data": "%s"%temp})

    print("Publishing...", obj)
    client.publish(topic, obj)
    time.sleep(3)