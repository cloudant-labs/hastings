import json
import requests

dbname = "countries"
countries = "data/countries.json"
baseUrl = "http://localhost:15984"
dbUrl = baseUrl + "/" + dbname
headers = {"content-type": "application/json"}
indexName = "countries"
designDocId = "_design/SpatialView"

designDoc = {"_id" : designDocId,
			"indexes" : {indexName : {
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
indexUrl = "%s/%s/_geo/%s?q=bbox(%f,%f,%f,%f)" % \
				(dbUrl, designDocId, indexName, \
					0, 51, 0, 51)
resp = requests.get(indexUrl)
print resp.json