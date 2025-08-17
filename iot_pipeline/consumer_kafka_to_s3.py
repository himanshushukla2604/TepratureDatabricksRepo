import os
import io
import gzip
import json
import time
import signal
from uuid import uuid4
from datetime import datetime

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from kafka import KafkaConsumer


class GracefulKiller:
    def __init__(self):
        self._kill_now = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, *_):
        self._kill_now = True

    @property
    def kill_now(self):
        return self._kill_now


def gzip_bytes(data: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(data)
    return buf.getvalue()


def flush_batch_to_s3(messages, s3_client, bucket, prefix):
    if not messages:
        return None

    # messages are strings of JSON; build NDJSON
    ndjson_str = "\n".join(messages) + "\n"
    compressed = gzip_bytes(ndjson_str.encode("utf-8"))

    now = datetime.utcnow()
    key = (
        f"{prefix}{now.strftime('%Y/%m/%d/%H')}/"
        f"batch_{now.strftime('%Y%m%dT%H%M%S')}_{len(messages)}_{uuid4().hex[:8]}.jsonl.gz"
    )

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=compressed,
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
    )
    return key


def main():
    # Kafka config
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC", "iot-sensor")
    group_id = os.getenv("KAFKA_GROUP_ID", "iot-s3-sink")

    # S3 config
    bucket_name = os.getenv("BUCKET_NAME", "my-iot-data-bucket-2025")
    s3_prefix = os.getenv("S3_PREFIX", "iot-data-batches/")

    # Batching config
    batch_size = int(os.getenv("BATCH_SIZE", "100"))
    max_wait_seconds = int(os.getenv("MAX_WAIT_SECONDS", "60"))

    # AWS client
    s3 = boto3.client("s3")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        group_id=group_id,
        value_deserializer=lambda v: v.decode("utf-8"),
        key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
        # allow iteration without blocking forever, we'll poll manually
        consumer_timeout_ms=0,
    )

    print(
        f"Consuming from topic='{topic}' on brokers '{bootstrap_servers}' with group_id='{group_id}'.\n"
        f"Will upload batches of {batch_size} messages to s3://{bucket_name}/{s3_prefix} (gzip NDJSON)."
    )

    killer = GracefulKiller()
    batch = []
    last_flush_ts = time.time()

    try:
        while not killer.kill_now:
            # poll returns a dict of TopicPartition -> list[ConsumerRecord]
            records_map = consumer.poll(timeout_ms=1000, max_records=500)

            total_polled = 0
            for _tp, records in (records_map or {}).items():
                for record in records:
                    total_polled += 1
                    # record.value is already decoded to utf-8 string
                    batch.append(record.value)

                    if len(batch) >= batch_size:
                        try:
                            key = flush_batch_to_s3(batch, s3, bucket_name, s3_prefix)
                            consumer.commit()
                            print(
                                f"Uploaded {len(batch)} messages to s3://{bucket_name}/{key} and committed offsets."
                            )
                            batch.clear()
                            last_flush_ts = time.time()
                        except (BotoCoreError, ClientError) as e:
                            print(f"S3 upload failed: {e}. Will retry after short backoff...")
                            time.sleep(5)
                            # do not commit; loop will retry on next iteration

            # Timed flush if we have accumulated some messages
            if batch and (time.time() - last_flush_ts) >= max_wait_seconds:
                try:
                    key = flush_batch_to_s3(batch, s3, bucket_name, s3_prefix)
                    consumer.commit()
                    print(
                        f"Time-based flush: uploaded {len(batch)} messages to s3://{bucket_name}/{key} and committed offsets."
                    )
                    batch.clear()
                    last_flush_ts = time.time()
                except (BotoCoreError, ClientError) as e:
                    print(f"S3 upload failed (time-based): {e}. Will retry after short backoff...")
                    time.sleep(5)

    except KeyboardInterrupt:
        pass
    finally:
        # Final flush on shutdown
        if batch:
            try:
                key = flush_batch_to_s3(batch, s3, bucket_name, s3_prefix)
                consumer.commit()
                print(
                    f"Shutdown flush: uploaded {len(batch)} messages to s3://{bucket_name}/{key} and committed offsets."
                )
            except (BotoCoreError, ClientError) as e:
                print(f"Final S3 upload failed: {e}. Offsets not committed to avoid data loss.")
        consumer.close()


if __name__ == "__main__":
    main()