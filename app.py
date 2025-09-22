from fastapi import FastAPI
from stream.sensor_stream_producer import SensorStreamProducer
from stream.stream_admin import SensorStreamAdmin
from config import brokers
from pydantic import BaseModel

class SensorData(BaseModel):
    factory_id: str
    sensor_id: str
    data: str

app = FastAPI()

admin = SensorStreamAdmin(brokers)
if admin.topic_exists(topic="factory-topic"):
    producer = SensorStreamProducer(brokers=brokers, topic="factory-topic")
else:
    raise Exception("Raise some topic error")

@app.post('/sensor')
async def send_sensor_data(sensor_data: SensorData):
    producer.write_message(factory_id=sensor_data.factory_id, sensor_id=sensor_data.sensor_id, data=sensor_data.data)