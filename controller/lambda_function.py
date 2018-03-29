import json
import logging
from time import time
from typing import Dict
from datetime import datetime as Datetime

from pynamodb.exceptions import PutError
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute, NumberAttribute

from jmespath import search

from shared.utils import send_message

logger = logging.getLogger()
logger.setLevel(logging.INFO)


class Jobs(Model):
    class Meta:
        table_name = 'Jobs'
        region = 'us-west-2'
        read_capacity_units = 5
        write_capacity_units = 5



    filename = UnicodeAttribute(hash_key=True)
    timestamp = NumberAttribute(range_key=True)
    datetime = UnicodeAttribute()
    worker = NumberAttribute()
    result = UnicodeAttribute(null=True)
    progress = NumberAttribute()


def record_job(key, worker, progress, result=None):
    timestamp = time() * 1_000
    try:
        job = Jobs(key, timestamp, worker=worker, result=result, progress=progress,
                   datetime=Datetime.utcnow().isoformat(timespec='milliseconds') + 'Z')
        job.save()
    except PutError as e:
        logger.error(str(e))


def handle_s3(record):
    bucket = search('s3.bucket.name', record)
    key = search('s3.object.key', record)
    size = search('s3.object.size', record)
    if size > 0:
        logger.info(f"Handling S3 trigger (file={key}, size={size})...")

        # Start 2 workers in parallel
        send_message('controller', 'worker1', 'StartJob', {"bucket": bucket,
                                                           "key": key,
                                                           'args': ['-s', '1920x1080']})

        send_message('controller', 'worker2', 'StartJob', {'bucket': bucket,
                                                           'key': key,
                                                           'args': ['-vf', 'transpose=1', '-s', '640x480']})


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

            logger.info(f"Handling message from {from_}: {msg}")

            if msg_type == 'JobProgress':
                key = msg.get('key')
                progress = int(msg.get('progress'))
                worker = int(from_[-1])
                record_job(key, worker, progress)
                return

            elif msg_type == 'JobCompleted':
                bucket = msg.get('bucket')
                key = msg.get('key')
                result = msg.get('result')
                worker = int(from_[-1])
                record_job(key, worker, 100 if result == 'Passed' else 0, result)

                if worker == 2:
                    # Worker 3 runs sequentially after worker 2
                    send_message('controller', 'worker3', 'StartJob', {'bucket': bucket,
                                                                       'key': key,
                                                                       'args': []})
                return


        elif s3 is not None:
            logger.info(f"Handling S3 trigger...")
            handle_s3(record)
            return
