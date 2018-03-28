import json
import logging
from typing import Dict

from shared.utils import send_message, run_ffmpeg, download_from_s3, upload_to_s3

logger = logging.getLogger()


class WorkError(Exception):
    pass


def perform_work(bucket: str, key: str):

    def progress_callback(progress: int):
        send_message('worker1', 'controller', 'JobProgress', {'bucket': bucket, 'key': key, 'progress': progress})

    progress_callback(0)
    input_file = download_from_s3(bucket, key)
    output_file, output = run_ffmpeg(1, input_file, ['-vf', 'transpose=1', '-s', '640x480'], progress_callback)
    upload_to_s3(bucket, key, output_file)


def lambda_handler(event: Dict, context: Dict):
    records = event.get('Records')
    if not records:
        logger.error("Invalid message!")
        return

    for record in records:
        sns = record.get('Sns')

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

            if msg_type == 'StartJob' and from_ == 'controller':
                bucket = msg.get('bucket')
                key = msg.get('key')

                try:
                    perform_work(bucket, key)
                    result = 'Passed'
                except WorkError as e:
                    result = 'Failed'

                send_message('controller', 'worker2', 'JobCompleted',
                             {'bucket': bucket,
                              'key': key,
                              'result': result})
