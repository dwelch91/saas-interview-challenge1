import re
import os
import json
from io import StringIO
from logging import getLogger
from pathlib import Path
from subprocess import PIPE, Popen
from typing import Dict

import boto3


logger = getLogger()


def send_message(from_: str, to: str, type_: str, data: Dict):
    sns = boto3.client('sns')
    payload = {"from": from_,
               "to": to,
               "type": type_}

    payload.update(data)

    attributes = {"to": {
        "DataTYpe": 'String',
        "StringValue": "controller"
        }
    }

    sns.publish(TargetArn="arn:aws:sns:us-west-2:730403596516:MainTopic",
                Message=json.dumps(payload),
                MessageAttributes=attributes)


def timestamp_to_secs(hr, min_, sec, msec):
    return int(hr) * 3600 + int(min_) * 60 + int(sec) + int(msec) / 1_000


ffmpeg_duration = re.compile(r"""Duration: (\d{2}):(\d{2}):(\d{2})\.(\d{2,3})""")
ffmpeg_time = re.compile(r"""time=(\d{2}):(\d{2}):(\d{2})\.(\d{2,3})""")


def run_ffmpeg(worker: int, input_file: Path, transform_args, progress_callback):
    aws = os.environ.get("AWS_EXECUTION_ENV")
    if aws is None:
        ffmpeg = '/usr/local/bin/ffmpeg'  # brew macOS
        output_file = Path('/tmp') / str(input_file.stem + f'.worker{worker}' + input_file.suffix)
    else:  # Amzn Linux Lambda
        task_dir = Path(os.environ.get("LAMBDA_TASK_ROOT"))
        ffmpeg = task_dir / 'ffmpeg'
        logger.info(ffmpeg)
        output_file = input_file.stem + f'.worker{worker}' + input_file.suffix

    logger.info(output_file)

    args = [str(ffmpeg), '-i', str(input_file), *transform_args, '-y', str(output_file)]
    logger.info(' '.join(args))
    percent_complete = 0
    progress_callback(percent_complete)

    stderr = StringIO()
    proc = Popen(args, universal_newlines=True, stderr=PIPE)
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
                progress_callback(percent_complete)

        if not line:
            break

    progress_callback(100)
    output = stderr.getvalue()
    stderr.close()
    return output_file, output


def download_from_s3(bucket: str, key: str):
    tmp = Path('/tmp')
    input_file = tmp / key
    s3 = boto3.client('s3')
    s3.Bucket(bucket).download_file(key, input_file)
    return input_file


def upload_to_s3(bucket: str, key: str, output_file: Path):
    s3 = boto3.client('s3')
    s3.Bucket(bucket).upload_file(output_file, key)