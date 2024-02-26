import asyncio

from reduct import Client, Bucket
from aiomqtt import Client as MQTTClient


async def main():
    async with Client("http://127.0.0.1:8383") as client:
        bucket: Bucket = await client.create_bucket('mqtt', exist_ok=True)

        async with MQTTClient("127.0.0.1") as mqtt:
            await mqtt.subscribe("#")
            async for message in mqtt.messages:
                await bucket.write(message.topic, message.payload)
                print(f"Received message {message.payload} from {message.topic} is written to the bucket")


if __name__ == "__main__":
    asyncio.run(main())
