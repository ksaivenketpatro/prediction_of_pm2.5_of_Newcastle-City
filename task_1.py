#import libraries
import requests
import json
from paho.mqtt import client as mqtt_client

# MQTT Broker Publisher
def emqx_publisher(payload):
    
    # IP adress of broker (Pre Set)
    mqtt_ip = "192.168.0.102"

    # Port number of broker (Pre Set)
    mqtt_port = 1883

    # Topic to subscribe (Pre Set)
    topic = "CSC8112"

    # Mesage to be sent
    msg = payload

    # Create a mqtt client object
    client = mqtt_client.Client()

    # Callback function for MQTT connection
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT OK!")
        else:
            print("Failed to connect, return code %d\n", rc)

    # Connect to MQTT service
    client.on_connect = on_connect
    client.connect(mqtt_ip, mqtt_port)

    # Publish message to MQTT
    # Note: MQTT payload must be a string, bytearray, int, float or None
    msg = json.dumps(msg)
    client.publish(topic, msg)


# URL Link for the API
api_url = "http://uoweb3.ncl.ac.uk/api/v1.1/sensors/PER_AIRMON_MONITOR1135100/data/json/?starttime=20220601&endtime=20220831"

# Request data from Urban Observatory Platform
resp = requests.get(api_url)

# Convert response(Json) to dictionary format
raw_data_dict = resp.json()

# Accessing PM2.5 Data
pm_data = raw_data_dict["sensors"][0]["data"]["PM2.5"]

# Initialize payload dictionary 
payload_data_dict = {"Timestamp":[],"Value":[]} 
for data in pm_data:
    payload_data_dict["Timestamp"].append(data["Timestamp"])
    payload_data_dict["Value"].append(data["Value"])
print(payload_data_dict)

# Send Payload to EMQX (MQTT Broker)
emqx_publisher(payload_data_dict)

