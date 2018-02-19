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
import argparse
import requests

parser = argparse.ArgumentParser(description='Run sample test `countries`')
parser.add_argument('--rm', dest='rm', action='store_true', help='Remove database after completion')
args = parser.parse_args()

dbname = "countries"
countries = "data/countries.json"
baseUrl = "http://localhost:15984"
dbUrl = baseUrl + "/" + dbname
headers = {"content-type": "application/json"}
indexName = "countries"
designDocId = "_design/SpatialView"

designDoc = {"_id" : designDocId,
			"st_indexes" : {indexName : {
				"index" : "function(doc){\
							if (doc.geometry) \
							{ \
								st_index(doc.geometry);\
							}\
						}"
					}
				}
			}

# # clean up any previous runs
requests.delete(dbUrl)
requests.put(dbUrl)
requests.post(dbUrl, data=json.dumps(designDoc), headers=headers)

# parse json feature collection
json_data = open(countries)
data = json.load(json_data)

for feature in data["features"]:
	Id = feature["id"]
	# delete feature type and duplicate id
	del feature["id"]
	del feature["type"]
	feature["_id"] = Id
	requests.post(dbUrl, data=json.dumps(feature), headers=headers)

json_data.close()

# trigger the index and query for the uk
indexUrl = "%s/%s/_geo/%s?bbox=%f,%f,%f,%f" % \
				(dbUrl, designDocId, indexName, \
					0, 51, 0, 51)
resp = requests.get(indexUrl)
if args.rm:
    requests.delete(dbUrl)
print resp.json()

resp.raise_for_status()
