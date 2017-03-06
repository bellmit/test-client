#! /usr/bin/env python 
import requests
from random import randint

url = "http://localhost:8080/pushservice/pointtopoint"
msgId = randint(10000, 60000)
message = {"msgID": msgId, "QoS": 1, "payload": "hey, man~~", "uid": "alex"}

print message 

resp = requests.session().post(url, json=message)
print vars(resp)


