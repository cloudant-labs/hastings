# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import json
import requests
import unittest


class Database(object):

    def __init__(self, adm, pwd, host, port):
        self.adm = adm
        self.pwd = pwd
        self.host = host
        self.port = port

    def get_stats(self):
        auth = "{}:{}".format(self.adm, self.pwd)
        addr = "{}:{}".format(self.host, self.port)
        return requests.get("http://{}@{}/_stats".format(auth, addr))


class TestMetrics(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.db = Database("foo", "bar", "localhost", "15986")

    def test(self):
        r = self.db.get_stats()
        assert (r.status_code == 200)

        metrics = json.loads(r.text)
        assert ("geo" in metrics.keys())

        geo = metrics["geo"]
        assert ("docs_processed" in geo.keys())
        assert ("emits" in geo.keys())
        assert ("crashes" in geo.keys())
