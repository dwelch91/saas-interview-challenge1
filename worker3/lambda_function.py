import json
import logging
from time import sleep
from typing import Dict, List

from shared.utils import send_message, WorkError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


WORKER = 'worker3'


def perform_work(bucket: str, key: str, args: List):
    sleep(2)
    raise WorkError("Simulate a failure in this lambda!")


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
