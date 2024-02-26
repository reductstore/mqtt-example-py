import asyncio
from reduct import Client, Bucket


async def read_data():
    async with Client("http://127.0.0.1:8383") as client:
        bucket: Bucket = await client.get_bucket('mqtt')
        for entry in await bucket.get_entry_list():
            async for record in bucket.query(entry.name):
                data = await record.read_all()
                print(f"Received message {data} with ts={record.timestamp} from {entry.name} is read from the bucket")


if __name__ == "__main__":
    asyncio.run(read_data())
