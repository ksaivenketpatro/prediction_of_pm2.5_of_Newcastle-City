# Import Libraries
import json
from paho.mqtt import client as mqtt_client
import pika
import json


# Function: To reject outlier values from the dataset
def outlier_func(data_values):

    #Assigning Values to distinct lists
    time_list = data_values["Timestamp"]
    value_list = data_values["Value"]

    # New Dictionary for the result 
    result_outlier={"Timestamp":[],"Value":[]}

    # for loop to remove outliers
    for i in range(len(value_list)):

        if value_list[i]<50:
            # Adding values to the result dict
            result_outlier["Value"].append(value_list[i])
            result_outlier["Timestamp"].append(time_list[i])

        else:
            print("Rejected Values: ",value_list[i])

    return result_outlier


# Function: TO determine the average of values
def avg_data(data_values):

    # Result Dictionary 
    avg_dict={"Timestamp":[],"Value":[]}

    # Assign values to distinct lists
    time_list = data_values["Timestamp"]
    value_list = data_values["Value"]

    # Temp Lists for processing
    val_new_list = []
    time_new_list = []

    # Cut Off - Time to determine the end of the day
    cut_off = time_list[0]+86400000

    # for loop to create average of values of a day
    for index in range(len(time_list)):

        if time_list[index] < cut_off:
            val_new_list.append(value_list[index])
            time_new_list.append(time_list[index])

            if time_list[index] == time_list[len(time_list)-1]:
                cut_off = time_list[index] + 86400000
                sum_val=0
                avg = 0
                for val in val_new_list:
                    sum_val = sum_val+val
                    avg = sum_val/len(val_new_list)
                avg_dict["Value"].append(avg)
                avg_dict["Timestamp"].append(time_new_list[0])

        else:
            cut_off = time_list[index] + 86400000
            sum_val = 0
            avg = 0
            for val in val_new_list:
                sum_val = sum_val+val
                avg = sum_val/len(val_new_list)
            avg_dict["Value"].append(avg)
            avg_dict["Timestamp"].append(time_new_list[0])
            val_new_list = []
            time_new_list = []
            val_new_list.append(value_list[index])
            time_new_list.append(time_list[index])

    return avg_dict


# Function : To send data to Rabbitmq broker
def rabbitmq_prodcer(data):

    # IP address of the rabbitmq broker
    rabbitmq_ip = "192.168.0.100"

    # Port Number of the rabbitmq broker
    rabbitmq_port = 5672

    # Queue name
    rabbitmq_queque = "CSC8112"

    # Message to be sent
    msg = data

    # Connect to RabbitMQ service
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_ip, port=rabbitmq_port))
    channel = connection.channel()

    # Declare a queue
    channel.queue_declare(queue=rabbitmq_queque)

    # Produce message
    channel.basic_publish(exchange='',
                        routing_key=rabbitmq_queque,
                        body=json.dumps(msg))
    connection.close()


# Function : function to retriev message by subscribing to a topic
def emqx_subscriber():

    # IP Address of the broker
    mqtt_ip ="192.168.0.102"

    # Port number of the broker
    mqtt_port = 1883

    # Topic of the message
    topic = "CSC8112"

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

    # Callback function will be triggered
    def on_message(client, userdata, msg):

        # Retrieve payload
        data = json.loads(msg.payload)

        # Process the payload 
        result_outlier = outlier_func(data)
        data = avg_data(result_outlier)
        print(data)

        # Send data to Rabbitmq message queing system (Cloud)
        rabbitmq_prodcer(data)
        
    # Subscribe MQTT topic
    client.subscribe(topic)
    client.on_message = on_message
    client.loop_forever()



