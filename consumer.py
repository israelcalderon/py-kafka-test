from aiokafka import AIOKafkaConsumer
import asyncio
import os
import sys


async def consume():
    bootstrap_server = os.environ.get('BOOTSTRAP_SERVER', 'localhost:9092')
    topic = os.environ.get('TOPIC', 'demo')
    group = os.environ.get('GROUP_ID', 'demo-group')
    consumer = AIOKafkaConsumer(
        topic, bootstrap_servers=bootstrap_server, group_id=group
    )

    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


def main():
    try:
        asyncio.run(consume())
    except KeyboardInterrupt:
        print("Bye!")
        sys.exit(0)


if __name__ == "__main__":
    print("Welcome to Kafka test script. ctrl + c to exit")
    main()
