

from kafka import KafkaProducer
import time
import csv
import pandas as pd
import json

def json_serializer(data):
 	return json.dumps(data).encode("utf-8")

producer = KafkaProducer(bootstrap_servers=['ip-172-31-80-55.ec2.internal:9092'],
                    	value_serializer=json_serializer)

with open('/home/ubuntu/kafka_2.13-3.1.1/SPY_TICK_TRADE.csv', newline=' ') as File:
	reader = csv.reader(File)
	for row in reader:
   		print(row)
   		producer.send("registered_user", row)
   		time.sleep(5)