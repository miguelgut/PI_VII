#!/usr/bin/env python
from datetime import datetime, timezone
from pyowm import OWM
from pyowm.utils import config
from pyowm.utils import timestamps
import config
import time
import json
import paho.mqtt.client as paho
import warnings
import random
import psycopg2

class App:	
	def __init__(self):
		pass

	def connectDB(self):
		db_user = config.db_user
		db_pass = config.db_pwd
		db_name = 'postgres'
		db_host = 'localhost'

		try:
			self.conn = psycopg2.connect(database=db_name,user=db_user,password=db_pass,host=db_host)
			self.cursor = self.conn.cursor()
		except:
			print("Unable to connect to the database")

	def createTable(self, tb_name):
		
		ct_string = "DROP TABLE IF EXISTS %s ;" % (tb_name) + "CREATE TABLE %s (id SERIAL PRIMARY KEY, dt TIMESTAMP WITH TIME ZONE, data JSONB, value NUMERIC(10,2))" % (tb_name)

		try:
			self.cursor.execute(ct_string)
			self.conn.commit()

		except psycopg2.OperationalError as e:
			print('\n{0}').format(e)


	def insertTable(self, tb_name, value, json):
		dt = datetime.now(timezone.utc)
		it_string = "INSERT INTO %s (dt, value, data) VALUES('%s', %s, '%s')" % (tb_name, dt, value, json)

		print(it_string)
		try:
			self.cursor.execute(it_string)
			self.conn.commit()

		except psycopg2.OperationalError as e:
			print('\n{0}').format(e)


	def apiPublish(self):
		warnings.filterwarnings("ignore")
		topic = 'PI7'

		client = paho.Client() 
		print("Connecting to broker ",config.broker)
		client.username_pw_set(config.user, password=config.pwd)
		client.connect(config.broker, 1883)

		# API Secret Key
		owm = OWM(config.owm_key)
		mgr = owm.weather_manager()

		while True:
			observation = mgr.weather_at_place('Pelotas,BR')
			w = observation.weather
			
			# temp = {"temp": w.temperature('celsius')['temp'], "date": observation.reception_time(timeformat='iso')}
			
			temp = w.temperature('celsius')['temp']
			obj = json.dumps({"id": "miguel-db-temp", "data": "%s"%temp})

			print("Publishing...", obj)
			client.publish(topic, obj)
			self.insertTable('miguel', temp, obj)
			time.sleep(3)

main = App()
main.connectDB()
main.createTable('miguel')
#main.insertTable('miguel', 10, '{"id": "oi"}')
main.apiPublish()