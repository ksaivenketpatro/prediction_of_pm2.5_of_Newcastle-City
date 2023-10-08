import json
from paho.mqtt import client as mqtt_client
if __name__ == '__main__':
    mqtt_ip = "192.168.0.102"
    mqtt_port = 1883
    topic = "CSC8112"
    msg = "wassup"
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
    print(msg)
    client.publish(topic, msg)
