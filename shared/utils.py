import logging
import re
import os
import json
from io import StringIO
from logging import getLogger
from pathlib import Path
from subprocess import PIPE, Popen, SubprocessError
from typing import Dict

import boto3


logger = getLogger()
logger.setLevel(logging.INFO)


class WorkError(Exception):
    pass


def send_message(from_: str, to: str, type_: str, data: Dict):
    sns = boto3.client('sns')
    payload = {"from": from_,
               "to": to,
               "type": type_}

    payload.update(data)

    attributes = {"to": {
        "DataType": 'String',
        "StringValue": to
        }
    }

    logger.info(f"Sending message from {from_} to {to}: {payload}...")
    sns.publish(TargetArn="arn:aws:sns:us-west-2:730403596516:MainTopic",
                Message=json.dumps(payload),
                MessageAttributes=attributes)


def timestamp_to_secs(hr, min_, sec, msec):
    return int(hr) * 3600 + int(min_) * 60 + int(sec) + int(msec) / 1_000


ffmpeg_duration = re.compile(r"""Duration: (\d{2}):(\d{2}):(\d{2})\.(\d{2,3})""")
ffmpeg_time = re.compile(r"""time=(\d{2}):(\d{2}):(\d{2})\.(\d{2,3})""")


def run_ffmpeg(worker: str, input_file: Path, transform_args, progress_callback):
    aws = os.environ.get("AWS_EXECUTION_ENV")
    if aws is None:
        ffmpeg = '/usr/local/bin/ffmpeg'  # brew macOS
        output_file = Path('/tmp') / (str(input_file.stem + f'.{worker}' + input_file.suffix))
    else:  # Amzn Linux Lambda
        task_dir = Path(os.environ.get("LAMBDA_TASK_ROOT"))
        ffmpeg = task_dir / 'ffmpeg'
        logger.info(ffmpeg)
        output_file = Path('/tmp') / (input_file.stem + f'.{worker}' + input_file.suffix)

    logger.info(input_file)
    logger.info(input_file.stat().st_size)
    logger.info(output_file)

    args = [str(ffmpeg), '-i', str(input_file), *transform_args, '-y', str(output_file)]
    logger.info(' '.join(args))
    percent_complete = 0
    prev_percent_complete = 0
    progress_callback(percent_complete)

    stderr = StringIO()
    try:
        proc = Popen(args, universal_newlines=True, stderr=PIPE)
    except SubprocessError as e:
        logger.error(f"Failed to run ffmpeg: {e}")
        raise WorkError("Failed to run ffmpeg!")

    duration = None
    while proc.poll() is None:
        line = proc.stderr.readline()
        stderr.write(line)
        if not duration:
            m = ffmpeg_duration.search(line)
            if m is not None:
                duration = timestamp_to_secs(*m.groups())

        if duration:
            m = ffmpeg_time.search(line)
            if m is not None:
                time = timestamp_to_secs(*m.groups())
                percent_complete = int(round(time / duration, 3) * 100)
                if percent_complete >= prev_percent_complete + 5:  # don't report more than 20times
                    progress_callback(percent_complete)
                    prev_percent_complete = percent_complete

        if not line:
            break

    result_code = proc.wait(5)
    output = stderr.getvalue()

    progress_callback(100)
    stderr.close()
    lines = output.splitlines()
    if len(lines) > 200:
        [logger.info(line) for line in lines[:100]]
        logger.info("--- snip ---")
        [logger.info(line) for line in lines[-100:]]
    else:
        [logger.info(line) for line in lines]

    logger.info(f"Result code: {result_code}")

    if result_code != 0:
         raise WorkError(f"ffmpeg failed with code {result_code}!")

    return output_file, output


def get_output_bucket(bucket: str):
    parts = bucket.split('.', 1)
    return f"{parts[0]}-output.{parts[1]}"


def get_output_key(worker: str, key: str):
    fn = Path(key)
    return fn.stem + f'.{worker}' + fn.suffix


def download_from_s3(bucket: str, key: str):
    tmp = Path('/tmp')
    input_file = tmp / key
    logger.info(f"Downloading from s3://{bucket}/{key} to {input_file}...")
    s3 = boto3.client('s3')
    s3.download_file(bucket, key, str(input_file))
    return input_file


def upload_to_s3(bucket: str, key: str, output_file: Path):
    logger.info(f"Uploading from {output_file} to s3://{bucket}/{key}...")
    s3 = boto3.client('s3')
    s3.upload_file(str(output_file), bucket, key)
