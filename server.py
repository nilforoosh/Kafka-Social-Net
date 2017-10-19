import os
import socket
from threading import Thread


#################################################################################

def listen():
	server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	host = "172.16.130.1"
	port = 3030

	server.bind((host,port))
	server.listen(5)

	while(True):
		client , addr = server.accept()
		name = client.recv(1024)
		topic = name.decode('ASCII')
		create_topic = "/home/mohammad/Kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic " + topic
		os.system(create_topic)
		client.close()

##############################################

th = Thread(target=listen)
th.start()

server_start = '/home/mohammad/Kafka/bin/kafka-server-start.sh /home/mohammad/Kafka/config/server.properties'
os.system(server_start)