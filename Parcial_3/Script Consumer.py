
from kafka import KafkaConsumer
import json

if _name_ == "_main_":
   consumer = KafkaConsumer(
  	"registered_user",
  	bootstrap_servers='ip-172-31-80-55.ec2.internal:9092',
           auto_offset_reset='earliest',
  	group_id="consumer-group-a")

print("starting the consumer")
for msg in consumer:
   print("Registered user = {}".format(json.loads(msg.value)))

)