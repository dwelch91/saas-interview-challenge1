import json
import logging
from typing import Dict, List

from shared.utils import send_message, run_ffmpeg, download_from_s3, upload_to_s3, get_output_bucket, WorkError, \
    get_output_key

logger = logging.getLogger()
logger.setLevel(logging.INFO)


WORKER = 'worker2'


def perform_work(bucket: str, key: str, args: List):

    def progress_callback(progress: int):
        logger.info(f"Progress: {progress}%")
        send_message(WORKER, 'controller', 'JobProgress', {'bucket': bucket, 'key': key, 'progress': progress})

    progress_callback(0)
    input_file = download_from_s3(bucket, key)
    output_file, output = run_ffmpeg(WORKER, input_file, args, progress_callback)
    output_bucket = get_output_bucket(bucket)
    output_key = get_output_key(WORKER, key)
    upload_to_s3(output_bucket, output_key, output_file)


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

            logger.info(f"Handling message from {from_}: {msg}")

            if msg_type == 'StartJob' and from_ == 'controller':
                bucket = msg.get('bucket')
                key = msg.get('key')
                args = msg.get('args')

                try:
                    perform_work(bucket, key, args)
                    result = 'Passed'
                except WorkError as e:
                    logger.error(str(e))
                    result = 'Failed'

                send_message(WORKER, 'controller', 'JobCompleted',
                             {'bucket': bucket,
                              'key': key,
                              'result': result})
