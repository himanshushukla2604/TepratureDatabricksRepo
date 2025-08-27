import boto3
import json
import time
import random
from datetime import datetime

# --- CONFIG ---
BUCKET_NAME = "my-iot-data-bucket-2025"  # replace with your bucket name
PREFIX = "iot-data/"  # folder in the bucket
INTERVAL_SECONDS = 5  # time between uploads

# Create S3 client
s3 = boto3.client("s3")

def generate_sensor_data():
    """Generate a random IoT sensor reading."""
    return {
        "device_id": f"sensor_{random.randint(100, 110)}",
        "timestamp": datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(-22, 80), 2),
        "humidity": round(random.uniform(-1, 101), 2),
        "battery": random.randint(-1, 102)
    }

while True:
    # Generate data
    data = generate_sensor_data()

    # Convert to JSON string
    json_str = json.dumps(data)

    # Create unique file path (date folders for organization)
    file_name = f"{PREFIX}{datetime.utcnow().strftime('%Y/%m/%d/%H%M%S')}_{data['device_id']}.json"

    # Upload to S3
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=json_str,
        ContentType="application/json"
    )

    print(f"Uploaded: {file_name} -> {json_str}")

    time.sleep(5)