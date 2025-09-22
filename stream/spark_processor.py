from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from ..config import brokers

spark = SparkSession \
        .builder \
        .appName('SensorStreamHandler') \
        .master('local') \
        .getOrCreate()

# structure of the data to consume
schema = T.StructType([
    T.StructField('')
])

sensorStream = spark.readStream.format('kafka').schema(schema=schema)\
                .option('kafka.bootstrap.servers', brokers)\
                .option('subscriber', "topic")\
                .option('startingOffsets', 'earliest')\
                .load()

## Process and Write Stream to Kafka or somewhere else
# prepared_sensor_data = sensorStream.groupBy('topic').agg().sort(F.asc('timestamp'))
# prepared_sensor_data.writeStream.format('kafka').outputMode('complete').start()