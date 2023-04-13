import json
import time
import calendar
import datetime
import random
from bson import json_util
from kafka import KafkaProducer


producer = KafkaProducer(bootstrap_servers='localhost:9093')

for i in range(100000):
    if i == 3: # emulate "wrong data"
        data = {
        'wrong_device': 'absolutely wrong data'
        }
    if i == 3: # emulate "wrong data"
        data2 = {
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
            'dev_id': 'C4:1G:1A:C2:42:12'
            }
        data2 = { # <----- sometimes rand's will generate unsuitable environment for the tortoise
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
            'dev_id': 'C2:11:1B:C1:42:12'
            }
    producer.send('tenant2-data', json.dumps(data, default=json_util.default).encode('utf-8'))
    producer.send('tenant2-data', json.dumps(data2, default=json_util.default).encode('utf-8'))

    #time.sleep(5) # turn off for extremely fast data speed.
