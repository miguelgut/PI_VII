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
		#self.filename = "/home/pospel-miguel/miguel.html"
		self.filename = "/var/www/html/miguel.html"
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
			#city = 'Pelotas,BR'
			#city = 'London,GB'
			city = 'Mumbai,IN'
			observation = mgr.weather_at_place(city)
			w = observation.weather
			# temp = {"temp": w.temperature('celsius')['temp'], "date": observation.reception_time(timeformat='iso')}
			temp = w.temperature('celsius')['temp']
			obj = json.dumps({"id": "miguel-db-temp", "data": "%s"%temp})

			print("Publishing...", obj)
			client.publish(topic, obj)
			self.insertTable('miguel', temp, obj)

			dt = datetime.now(timezone.utc)
			self.writeFile(temp, dt)
			time.sleep(3)

	def writeFile(self, temp, dt):
		arquivo = open(self.filename, "w")
		arquivo.close()
		arquivo = open(self.filename, "a")
		linha_arq = "<!DOCTYPE html>" + " \n"
		arquivo.write(linha_arq)
		linha_arq = "<html>" + " \n"
		arquivo.write(linha_arq)
		linha_arq = "  <head>" + " \n"
		arquivo.write(linha_arq)
		linha_arq = "    <meta charset='utf-8'>" + " \n"
		arquivo.write(linha_arq)
		linha_arq = "    <title>Ultimas Medicoes</title>" + " \n"
		arquivo.write(linha_arq)
		linha_arq = "    <meta http-equiv='refresh' content='60'>" + " \n"
		arquivo.write(linha_arq)
		linha_arq = "  </head>" + " \n"
		arquivo.write(linha_arq)
		linha_arq = "  <body>" + " \n"
		arquivo.write(linha_arq)
		linha_arq = "<table><tbody>" + " \n"
		arquivo.write(linha_arq)
		linha_arq = "<tr><td>" + str(temp) + "</td><td>" + str(dt) + " </td></tr>"
		arquivo.write(linha_arq)
		linha_arq = "</tbody></table>"
		arquivo.write(linha_arq)
		linha_arq = "  </body>" + " \n"
		arquivo.write(linha_arq)
		linha_arq = "</html>" + " \n"
		arquivo.write(linha_arq)
		arquivo.close()

main = App()
#main.connectDB()
#main.createTable('miguel')
#main.insertTable('miguel', 10, '{"id": "oi"}')
main.apiPublish()