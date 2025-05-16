from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import sys
import json

myMQTTClient = AWSIoTMQTTClient("DavitRaspberry")

myMQTTClient.configureEndpoint("d09530762c0nb5dceftxl-ats.iot.us-east-1.amazonaws.com", 8883)
myMQTTClient.configureCredentials("./AmazonRootCA1.pem","./private.pem.key", "./certificate.pem.crt")

myMQTTClient.connect()
print("Client Connected")

msg = {"msg": "Sample data from the device"}
topic = "Davit/pms5003/data"
myMQTTClient.publish(topic, json.dumps(msg), 0)
print("Message Sent")

myMQTTClient.disconnect()
print("Client Disconnected")
