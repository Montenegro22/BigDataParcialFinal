

from kafka import KafkaConsumer
import json

if _name_ == "_main_":
   consumer = KafkaConsumer(
  	"registered_user",
  	bootstrap_servers='ip-172-31-80-55.ec2.internal:9092',
  	auto_offset_reset='earliest',
  	group_id="consumer-group-a")
print("starting the consumer")

#max = 1420000
#min = 1400000

max = 1407000
min = 1406100
for msg in consumer:
   line = format(json.loads(msg.value)[1])
   if line != 'PRICE':
 	value = int(line)
 	if value > max:
    	    print("Alert!! New Max Value: ", value)
 	elif value < min:
    	     print("Alert!! New Min Value: ", value)