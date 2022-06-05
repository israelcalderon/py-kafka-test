from aiokafka import AIOKafkaProducer
import asyncio
import os
import sys


async def produce_message(message: str):
    bootstrap_server = os.environ.get('BOOTSTRAP_SERVER', 'localhost:9092')
    topic = os.environ.get('TOPIC', 'demo')
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_server)
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        # Produce message
        await producer.send_and_wait(topic, bytes(message, "UTF-8"))
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


def main():
    try:
        while True:
            message = input("Give me a message: ")
            asyncio.run(produce_message(message))
    except KeyboardInterrupt:
        print('Bye!')
        sys.exit(0)


if __name__ == "__main__":
    print("Welcome to Kafka test script. ctrl + c to exit")
    main()
