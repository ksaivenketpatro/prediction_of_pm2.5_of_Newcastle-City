# Import Libraries
import json
import pika
import os
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt

# Import ML Engine
from ml_engine import MLPredictor


# Function : To plot the data set 
def plot_pic(data_df):

    # Initialize a canvas
    plt.figure(figsize=(8, 4), dpi=200)
    # Plot data into canvas
    plt.plot(data_df["Timestamp"], data_df["Value"], color="#FF3B1D", marker='.', linestyle="-")
    plt.xticks(data_df["Timestamp"],rotation=90)
    plt.title("Example data for demonstration")
    plt.xlabel("DateTime")
    plt.ylabel("Value")
    # Save as file
    plt.savefig(os.path.join("C:/", "hi1"))
    # Directly display
    plt.show()


# Function : To Create a ML Engine specific dataframe
def ml_data(data):

    # for loop to retrieve and convert the timestamp 
    for index in range(len(data["Timestamp"])):
        data["Timestamp"][index] = datetime.fromtimestamp(data["Timestamp"][index]/1000)

    # Create and save the data as a dataframe
    ml_df = pd.DataFrame.from_dict(data)

    return ml_df


#Function : To Display the dataset and forecast the PM2.5 Data 
def ml_engine(data):

    # Create dataset for training 
    ml_df = ml_data(data)

    # Plot the dataset 
    plot_pic(ml_df)

    # Train the dataset
    predictor = MLPredictor(ml_df)
    predictor.train()

    # Predict the datset
    forecast = predictor.predict()

    # Display and Save the forecast 
    fig = predictor.plot_result(forecast)
    fig.savefig(os.path.join("C:/", "hi"))

# Function : To retreive data from the rabbitmq broker and initiate the Forecasting of the PM2.5 Data
def rabbitmq_consumer():
    
    # IP address of the rabbitmq broker
    rabbitmq_ip = "192.168.0.100"

    # Port Number of the rabbitmq broker
    rabbitmq_port = 5672

    # Queue name
    rabbitmq_queque = "CSC8112"

    # Callback function to retrieve the message
    def callback(ch, method, properties, body):

        # Start the ML Engine
        ml_engine(json.loads(body))

    # Connect to RabbitMQ service with timeout 1min
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_ip, port=rabbitmq_port, socket_timeout=60))
    channel = connection.channel()

    # Declare a queue
    channel.queue_declare(queue=rabbitmq_queque)
    channel.basic_consume(queue=rabbitmq_queque,
                        auto_ack=True,
                        on_message_callback=callback)

    channel.start_consuming()


# To run the program
rabbitmq_consumer()