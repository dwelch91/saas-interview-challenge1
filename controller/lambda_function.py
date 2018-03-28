import json
import logging
from time import time
from typing import Dict

from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute

from jmespath import search

from shared.utils import send_message

logger = logging.getLogger()


class Jobs(Model):
    class Meta:
        table_name = 'Workflows'
        region = 'us-west-2'
        read_capacity_units = 10
        write_capacity_units = 10

    path = UnicodeAttribute(hash_key=True)
    timestamp = NumberAttribute(range_key=True)
    worker = NumberAttribute()
    result = UnicodeAttribute(null=True)
    progress = NumberAttribute()


def record_job(bucket, key, worker, progress, result=None):
    timestamp = time() * 1_000
    job = Jobs(f"s3://{bucket}/{key}", timestamp, worker=worker, result=result, progress=progress)
    job.save()


def handle_s3(record):
    bucket = search('s3.bucket.name', record)
    key = search('s3.object.key', record)
    send_message('controller', 'worker1', 'StartJob', {"bucket": bucket, "key": key})


def lambda_handler(event: Dict, context: Dict):
    records = event.get('Records')
    if not records:
        logger.error("Invalid message!")
        return

    for record in records:
        sns = record.get('Sns')
        s3 = record.get('s3')

        if sns is not None:
            message = sns.get('Message')
            try:
                msg = json.loads(message)
            except (TypeError, ValueError) as e:
                logger.error("Invalid message, must be JSON!")
                return

            msg_type = msg.get('type')
            from_ = msg.get('from')

            logger.info(f"Handling message from {from_}...")

            if msg_type == 'JobProgress':
                bucket = msg.get('bucket')
                key = msg.get('key')
                progress = int(msg.get('progress'))
                record_job(bucket, key, 1, progress)

            if msg_type == 'JobCompleted' and from_ == 'worker1':
                bucket = msg.get('bucket')
                key = msg.get('key')
                result = msg.get('result')
                record_job(bucket, key, 1, 100 if result == 'Passed' else 0, result)
                send_message('controller', 'worker2', 'StartJob', {'bucket': bucket, 'key': key})

            elif msg_type == 'JobCompleted' and from_ == 'worker2':
                bucket = msg.get('bucket')
                key = msg.get('key')
                result = msg.get('result')
                record_job(bucket, key, 2, 100 if result == 'Passed' else 0, result)
                send_message('controller', 'worker3', 'StartJob', {'bucket': bucket, 'key': key})

            elif msg_type == 'JobCompleted' and from_ == 'worker3':
                bucket = msg.get('bucket')
                key = msg.get('key')
                result = msg.get('result')
                record_job(bucket, key, 3, 100 if result == 'Passed' else 0, result)

        elif s3 is not None:
            handle_s3(record)
