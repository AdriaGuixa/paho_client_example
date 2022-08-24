"""
Main function of the MQTT client example

:author: Adria Guixa
:since: 2022-08-18
"""
import paho.mqtt.client as mqtt
import json
import logging

BROKER_ADDRESS = "rat.rmq2.cloudamqp.com"
BROKER_PORT = 1883
USERNAME = "username"
PASSWORD = "password"


def on_message(client, userdata, message):
    logging.info("message received {}".format(str(message.payload.decode("utf-8"))))
    logging.info("message topic={}".format(message.topic))
    logging.info("message qos={}".format(message.qos))
    logging.info("message retain flag={}".format(message.retain))
    d = json.loads(message.payload.decode("utf-8"))
    with open("file.txt", "a+") as file:
        file.write("%s\n" % d)


def on_disconnect(client, rc=0):
    logging.info("Disconnected result code "+str(rc))
    client.loop_stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info("creating new instance")
    client = mqtt.Client("P1")  # create new instance
    client.on_message = on_message  # attach function to callback
    client.username_pw_set(USERNAME, PASSWORD)
    logging.info("connecting to broker")
    client.connect(host=BROKER_ADDRESS, port=BROKER_PORT)  # connect to broker
    try:
        logging.info("Subscribing to topic: {}".format("topic"))
        client.subscribe("topic")
        client.loop_forever()  # start the loop
    except KeyboardInterrupt:
        logging.info("Client disconnected")
        on_disconnect(client)
