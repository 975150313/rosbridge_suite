#!/usr/bin/env python

import json
import signal
import time

import paho.mqtt.client as mqtt
import rospy
from loguru import logger
from rosbridge_library.rosbridge_protocol import RosbridgeProtocol

# The following parameters are passed on to RosbridgeProtocol
# defragmentation.py:
from roslibpy.conversions import to_epoch

fragment_timeout = 600  # seconds
# protocol.py:
delay_between_messages = 0  # seconds
max_message_size = None  # bytes
unregister_timeout = 10.0  # seconds

advertised_topics = []
ages = []

import numpy as np

def process_message_from_mqtt_side(client, _, msg):
    """
    Forward a message coming from the MQTT Client side to the ROS Bridge. This
    function is a callback that is invoked by the handler's MQTT client upon
    messages incoming on the MQTT topic "rosbridge".

    If a message is to be published by the client, it has got to be sent to the
    "rosbridge" topic, enveloped in a rosbridge protocol message with
    "op": "publish".

    :param client: The mqtt client that handles this message
    :param _: unknown, see paho.mqtt documentation
    :param msg: The message as string; usually contains a JSON encoded object
    """
    global first_age
    try:
        payload = msg.payload.decode()
        obj = json.loads(payload)
        if "msg" in obj and "data" in obj["msg"]:
            msg_payload = json.loads(obj["msg"]["data"])
            obj["msg"]["data"] = msg_payload

        payload = json.dumps(obj)
        ret = rb_prot.incoming(payload)
    except Exception as e:
        logger.error(f"Error publishing message to ROS Bridge: {str(e)}")


def process_message_from_ros_side(message=None):
    """
    Process messages coming in on the subscribed ROS topics. They are enveloped
    in a rosbridge operation with "op": "publish".

    :param message: The message as string; usually contains a JSON encoded
                    object
    """
    obj = json.loads(message)
    topic = obj["topic"]
    msg = obj["msg"]
    if not isinstance(msg, str):
        if "header" in msg:
            stamp = to_epoch(msg["header"]["stamp"])
            age = time.time() - stamp

            ages.append(age)
            if len(ages) > 10:
                del ages[0]

            logger.info(f"Age Variance: {np.var(ages):6.3f}")

        msg = json.dumps(msg).encode()
    try:
        mqtt_controller.publish(topic, msg)
    except Exception as e:
        logger.error(f"Error publishing message to MQTT: {str(e)}")


mqtt_controller = mqtt.Client("rosbridge_mqtt")
mqtt_controller.on_message = process_message_from_mqtt_side
mqtt_controller.connect("localhost")
mqtt_controller.subscribe("rosbridge")
mqtt_controller.loop_start()

if __name__ == "__main__":
    rospy.init_node("mqtt_bridge")

    parameters = {
        "fragment_timeout": fragment_timeout,
        "delay_between_messages": delay_between_messages,
        "max_message_size": max_message_size,
        "unregister_timeout": unregister_timeout
    }

    rb_prot = RosbridgeProtocol(0, parameters=parameters)
    rb_prot.outgoing = process_message_from_ros_side


    def shutdown(*args, **kwargs):
        logger.info("Shutting down")
        mqtt_controller.loop_stop()
        exit(0)


    rospy.on_shutdown(shutdown)
    signal.signal(signal.SIGINT, lambda x, y: rospy.signal_shutdown("keyboard interrupt"))

    rospy.spin()
