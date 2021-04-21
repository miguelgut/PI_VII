import time
import random
# Importa o publish do paho-mqtt
import paho.mqtt.client as mqtt
client = mqtt.Client()


# Device ID do Konker
user = "qnplshrpa0hp"
# Password do Konker
pwd = "BDvMNwDDIV1W"

client.username_pw_set(user, pwd)

# Numero do campo (field) que deseja enviar o valor
channel = "teste_01"

# Broker do Konker
client.connect("mqtt.demo.konkerlabs.net", 1883)

topic = "data/qnplshrpa0hp/pub/" + channel

contador = 0
while (contador < 60):
	contador = contador + 1
	# Valor a enviar
	valor = random.randint(0,999)
	# Publica ao canal
	print("Enviando valor: " + str(valor))
	print("contador: " + str(contador))
	client.publish(topic, valor)
	time.sleep(5)