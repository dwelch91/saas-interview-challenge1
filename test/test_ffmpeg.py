from io import StringIO
from pathlib import Path
from subprocess import TimeoutExpired, Popen, PIPE
from unittest import TestCase

import re

from shared.utils import timestamp_to_secs, run_ffmpeg

ffmpeg_duration = re.compile(r"""Duration: (\d{2}):(\d{2}):(\d{2})\.(\d{2,3})""")
ffmpeg_time = re.compile(r"""time=(\d{2}):(\d{2}):(\d{2})\.(\d{2,3})""")


class TestFfmpeg(TestCase):
    def test_ffmpeg(self):
        root_dir = Path(__file__).parents[1]
        bin_dir = root_dir / 'bin'
        samples_dir = root_dir / 'samples'

        #ffmpeg = bin_dir / 'ffmpeg'  # Amazon Linux
        ffmpeg = '/usr/local/bin/ffmpeg'  # brew on MacOS
        sample = samples_dir / 'Bars-1280x720-29.97-DF.mp4'
        tmp = Path('/tmp')
        output = tmp / 'sample.mp4'
        args = [str(ffmpeg), '-i', str(sample), '-s', '640x480', '-y', str(output)]
        #args = [str(ffmpeg), '-i', str(sample), '-vf', 'vflip', '-c:a', 'copy', '-y', str(output)]
        #args = [str(ffmpeg), '-i', str(sample), '-vf', 'transpose=1', '-c:a', 'copy', '-y', str(output)]
        print(' '.join(args))
        print(args)

        stderr = StringIO()

        proc = Popen(args, universal_newlines=True, stderr=PIPE)

        print(proc.poll())
        percent_complete = 0
        #out = []
        duration = None
        while proc.poll() is None:
            line = proc.stderr.readline()
            stderr.write(line)
            #print(line)
            if not duration:
                m = ffmpeg_duration.search(line)
                if m is not None:
                    duration = timestamp_to_secs(*m.groups())
                    print(f"Duration: {duration}")

            if duration:
                m = ffmpeg_time.search(line)
                if m is not None:
                    time = timestamp_to_secs(*m.groups())
                    print(f"Time: {time}")

                    percent_complete = int(round(time / duration, 3) * 100)
                    print(percent_complete)

            if not line:
                break

        # print(proc.poll())
        percent_complete = 100
        print(stderr.getvalue())

        # try:
        #     outs, errs = proc.communicate(timeout=15)
        # except TimeoutExpired:
        #     proc.kill()
        #     outs, errs = proc.communicate()

    def test_ffmpeg_util(self):

        def progress(percent: int):
            print(f"{percent}%")

        root_dir = Path(__file__).parents[1]
        samples_dir = root_dir / 'samples'
        sample = samples_dir / 'Bars-1280x720-29.97-DF.mp4'
        output_file, output = run_ffmpeg('worker1', sample, ['-s', '640x480'], progress)
        print(output_file)
        print(output)
