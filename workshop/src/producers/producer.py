import dataclasses
import json
import sys
import time
from pathlib import Path

# Add workshop/src to the import path so we can import local modules
# when running this script directly (outside a package).
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer
from models import Ride, ride_from_row

# Download NYC yellow taxi trip data (first 1000 rows).
# Pandas reads the Parquet file directly from the URL and selects only the columns we need.
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
columns = ['PULocationID', 'DOLocationID', 'trip_distance', 'total_amount', 'tpep_pickup_datetime']
df = pd.read_parquet(url, columns=columns).head(1000)

# Kafka expects bytes, so we serialize our Ride dataclass to JSON bytes.
def ride_serializer(ride):
    # dataclasses.asdict converts the dataclass instance to a plain dict.
    ride_dict = dataclasses.asdict(ride)
    # json.dumps turns the dict into a JSON string.
    json_str = json.dumps(ride_dict)
    # .encode converts the string to bytes (Kafka's wire format).
    return json_str.encode('utf-8')

# Kafka broker address (host:port).
server = 'localhost:9092'

# Create a producer that will serialize each message with our function above.
producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=ride_serializer
)
t0 = time.time()

topic_name = 'rides'

# Iterate over the dataframe rows, convert each row to a Ride object, and send it.
for _, row in df.iterrows():
    ride = ride_from_row(row)
    # producer.send is asynchronous: it queues the message for sending.
    producer.send(topic_name, value=ride)
    print(f"Sent: {ride}")
    # Small delay to avoid flooding the broker in a demo setting.
    time.sleep(0.01)

# Ensure all queued messages are actually sent before the script exits.
producer.flush()

t1 = time.time()
# Measure total time spent sending the 1000 messages.
print(f'took {(t1 - t0):.2f} seconds')
