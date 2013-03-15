import json
import requests

dbname = "colorado_skiing"
areas = "data/ski_areas.json"
baseUrl = "http://localhost:15984"
dbUrl = baseUrl + "/" + dbname
headers = {"content-type": "application/json"}
indexName = "ski_areas"
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
print "DB PUT %s" % (dbUrl)
requests.post(dbUrl, data=json.dumps(designDoc), headers=headers)

# parse json feature collection
json_data = open(areas)
data = json.load(json_data)

for feature in data["features"]:
	Id = feature["id"]
	# delete feature type and duplicate id
	del feature["id"]
	feature["_id"] = str(Id)
	print json.dumps(feature)
	requests.post(dbUrl, data=json.dumps(feature), headers=headers)

json_data.close()

# trigger the index and query for CO
indexUrl = "%s/%s/_geo/%s?bbox=%f,%f,%f,%f" % \
				(dbUrl, designDocId, indexName, \
					-107.70996093749999,38.71123253895224,-104.534912109375,40.613952441166596)
resp = requests.get(indexUrl)
print resp.json
