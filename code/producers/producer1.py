import json
import time
import calendar
import datetime
import random
from bson import json_util
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers='localhost:9093')

#time,readable_time,acceleration,acceleration_x,acceleration_y,acceleration_z,battery,humidity,pressure,temperature,dev-id
#1522826433358,2018-04-04T07:20:33.358000Z,1009.8237469974649,-104,332,948,3007,20,1009.91,35.24,C2:9A:9F:E5:58:27

for i in range(100000):
    if i == 3: # emulate "wrong data"
        data = {
        'wrong_device': 'absolutely wrong data'
        }
    else:
        data = { # <----- sometimes rand's will generate unsuitable environment for the tortoise
            'time': calendar.timegm(time.gmtime()),
            'readable_time' : datetime.datetime.now().isoformat(),
            'acceleration' : round(random.uniform(0,1000),5),
            'acceleration_x': round(random.uniform(-1000,1000),1),
            'acceleration_y': round(random.uniform(-1000,1000),1),
            'acceleration_z': round(random.uniform(-1000,1000),1),
            'battery': random.randint(0,100),
            'humidity': random.randint(97,100),
            'pressure': round(random.uniform(999,1005),2),
            'temperature': round(random.uniform(15,31),2),
            'dev_id': 'C2:9A:9F:E5:58:27'
            }
    producer.send('tenant1-data', json.dumps(data, default=json_util.default).encode('utf-8'))

    #time.sleep(5) # turn off for extremely fast data speed.
