import json
import time
import random
from datetime import datetime, timedelta
from google.cloud import pubsub_v1

#place these with your actual project ID and topic name
project_id = "iot-sensor-462716"
topic_id = "iot_sensor"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

sensor_ids = ["sensor-1", "sensor-2", "sensor-3"]
locations = ["california", "Washington", "florida"]

print("Publishing sensor data to Pub/Sub...")

for i in range(100):
    data = {
        "sensor_id": random.choice(sensor_ids),
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(25.0, 40.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
        "location": random.choice(locations)
    }
    message = json.dumps(data).encode("utf-8")
    future = publisher.publish(topic_path, message)
    print(f"Published: {data}")
    time.sleep(1)
