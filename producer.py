from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import datetime
import avro.schema
import io
import random
from avro.io import DatumWriter


def encode(schema_file, data):
    raw_bytes = None
    try:
        schema = avro.schema.Parse(open(schema_file).read())
        writer = avro.io.DatumWriter(schema)
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(data, encoder)
        raw_bytes = bytes_writer.getvalue()
    except:
        print("Error encode data")
    return raw_bytes


async def send_one(loop):
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers='localhost:9092')
    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        x = 0
        while True:
            x = x + 1
            # Produce message
            await asyncio.sleep(1.5)
            user_data = {

                "id": x,
                "name": "Cuong Ba Ngoc",
                "address": "Ha Noi - Viet Nam"
            }
            raw_bytes = encode('user.avsc', user_data)
            await producer.send_and_wait("scott", raw_bytes)
            print('.')
    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()
    print('DDDSSSDDD')


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    send_task = asyncio.async(send_one(loop))
    loop.run_forever()
