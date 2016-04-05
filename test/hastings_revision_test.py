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

    def __init__(self, adm, pwd, host, port, db, ddoc, idx):
        self.adm = adm
        self.pwd = pwd
        self.host = host
        self.port = port
        self.db = db
        self.ddoc = ddoc
        self.idx = idx

    def get_result(self):
        auth = "{}:{}".format(self.adm, self.pwd)
        addr = "{}:{}".format(self.host, self.port)
        path = "{}/{}/_geo/{}".format(self.db, self.ddoc, self.idx)
        para = "{}&{}&{}".format("format=view", "bbox=-180,-90,180,90", "limit=1")
        return requests.get("http://{}@{}/{}?{}".format(auth, addr, path, para))

 
class TestGoeSearch(unittest.TestCase):

    @classmethod
    def setUp(cls):
        cls.db = Database("foo", "bar", "localhost", "5984", "testdb", "_design/geodd", "geoidx")

    def test(self):
        r = self.db.get_result()
        assert (r.status_code == 200)

        result = json.loads(r.text)
        assert ("rows" in result.keys())

        rows = result["rows"]
        assert ("rev" in rows[0].keys())

if __name__ == '__main__':
    unittest.main()
