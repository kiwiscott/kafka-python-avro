from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
import asyncio
import datetime
from concurrent.futures import ProcessPoolExecutor
import avro.schema
import avro.io
import io

schema = avro.schema.Parse(open('user.avsc').read())


def decode(msg):
    bytes_reader = io.BytesIO(msg.value)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)


async def consume(loop, d):
    consumer = AIOKafkaConsumer(
        'scott', 'test',
        loop=loop, bootstrap_servers='localhost:9092')
    # Get cluster layout and join group `my-group`
    await consumer.start()
    # await consumer.seek_to_beginning()
    try:
        while True:
            async for msg in consumer:
                user = decode(msg)
                print("consumed: ", d, msg.topic, msg.partition, msg.offset,
                      msg.timestamp, msg.value, user)
    finally:
        await consumer.stop()
    print('ASSS')

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    tasks = []
    for x in range(5):
        tasks.append(asyncio.async(consume(loop, 'task' + str(x))))

    loop.run_forever()
