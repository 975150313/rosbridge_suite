import asyncio
import json
import time

import paho
from loguru import logger
from paho.mqtt.client import Client
from paho.mqtt.subscribe import simple

client = Client()
client.connect("localhost")


async def send():
    seq = 0
    try:
        while True:
            s = time.time()
            secs = int(s)
            nsecs = int((s - secs) * 1e9)

            hdr = {
                "frame_id": "test",
                "seq": seq,
                "stamp": {
                    "secs": secs,
                    "nsecs": nsecs
                }
            }

            op = {
                "op": "publish",
                "topic": "/test_hdr",
                "msg": hdr
            }
            msg = json.dumps(op)

            client.publish("/test_hdr", msg)
            await asyncio.sleep(1. / 1000.)
    except Exception as e:
        logger.error(f"Sender exited: {str(e)}")
        return


from concurrent.futures import ThreadPoolExecutor
import numpy as np

async def receive():
    try:
        ages = []
        while True:
            msg = await loop.run_in_executor(ThreadPoolExecutor(),
                                             lambda: simple("/people_tracking/people"))
            payload = json.loads(msg.payload.decode())
            topic = msg.topic
            logger.info(f"Received message on topic {topic}")
            header = payload["header"]
            stamp = header["stamp"]
            secs = stamp["secs"]
            nsecs = stamp["nsecs"]
            posix = secs + nsecs * 1e-9

            age = time.time() - posix
            ages.append(age)
            if len(ages) > 10:
                del ages[0]
            age_variance = np.var(ages)

            logger.info("Age of message changed by: "
                        f"{age_variance:9.6f} seconds")

            await asyncio.sleep(1. / 5.)
    except Exception as e:
        logger.error(f"Receiver exited: {str(e)}")
        return


subscription = {"op": "subscribe", "topic": "/people_tracking/people_json", "type": "std_msgs/String"}
client.publish("rosbridge", json.dumps(subscription))

loop = asyncio.get_event_loop()
# loop.create_task(send())
loop.create_task(receive())
loop.run_forever()
