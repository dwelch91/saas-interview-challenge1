
from unittest import TestCase


from shared.utils import get_output_bucket, get_output_key


class TestShared(TestCase):
    def test_get_output_bucket(self):
        self.assertEqual("sandbox-output.dwelch91.org", get_output_bucket("sandbox.dwelch91.org"))


    def test_get_output_key(self):
        self.assertEqual("GoldenDemoContentDon_2.worker1.mxf", get_output_key('worker1', 'GoldenDemoContentDon_2.mxf'))