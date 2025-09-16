import aiokafka
import asyncio
import time
import json

async def main():

    consumer = aiokafka.AIOKafkaConsumer(
        "FirstTopic",
        bootstrap_servers='localhost:9092'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(
                "{}:{:d}:{:d}: key={} value={} timestamp_ms={}".format(
                    msg.topic, msg.partition, msg.offset, msg.key, msg.value,
                    msg.timestamp)
            )
            with open("output.json", "w") as outfile:
                json.dump(json.loads(msg.value), outfile, indent=4)
    finally:
        await consumer.stop()


# asyncio.run(main())
#
# while True:
#     print('working...')
#     time.sleep(3)
#

def loop_in_thread(loop):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())

loop = asyncio.get_event_loop()
import threading
t = threading.Thread(target=loop_in_thread, args=(loop,))
t.start()

while True:
    print('working...')
    time.sleep(3)