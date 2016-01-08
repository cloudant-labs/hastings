# Cloudant Query Geospatial

## Overview


This is an ongoing requirements document describing the user scenarios and syntax for adding geospatial support to Cloudant Query.

## Index Creation

We will continue using the Cloudant Query index creation structure. The simplest format will be as follows:

    {
	    "index": {
		    "fields": ["geometry", "location"]
	    },
	    "name": "foo-index",
	    "type": "geo"
    }

This essentially indexes GeoJson objects referenced by the fields "geometry" and "location". At the Cloudant Query indexing level, we ensure that the object is valid GeoJson.

A possible alternative could be a stricter definition such as:

    {
        "index": {
            "fields": [
                {"geometry": "Point"},  
                {"location": "Polygon"}
            ]
    },
        "name": "foo-index",
        "type": "geo"
    }

In this case, Cloudant Query will ensure that the "type" field in the GeoJson object matches the type provided by the user.

## Query Syntax

**The following operators are suggested for the firstversion and subject to change.**

#####GeoQuery Operators

These GeoQuery operators are the top level operators that will be used in conjunction with other Cloudant Query operators. They cannot be nested within other Cloudant Query operators. Examples will be shown in the usage section.

**$geoContains** - Given a query geometry Q, we return all documents whose GeoJson Object R are within Q, including the border.

**$geoWithin** - Opposite of $geoContains. Returns all documents whose GeoJson Object R contains the query geometry Q.

**$geoIntersects** - Given a query geometry Q, we return all documents whose GeoJson Object R intersects with Q.

**$geoNear** - Given a query geometry Q, we return all documents sorted by distance calculated from the center point of Q.


#####Geometry Operators

The geometry operators are nested within one of the 4 GeoQuery operators above.

**$ellipse** - Specifies an ellipse shape with longitude, latitude, and two radii. It's value will be an array of 4 objects that contains the **$lat, $lon, $rangex, $rangey **operators:

    {
        "$ellipse" : [
            {"$lon": 12.0223},
            {"$lon": 10032132},
            {"$rangex": 50},
            {"$rangey": 25}
        ]
    }
**$radius** - Specifies a circle shape with longitude, latitude, and radius. It's value will be an array of 3 objects that contains the **$lat, $lon, $rangex, $radius **operators:

    {
        "$radius" : [
            {"$lon": 12.0223},
            {"$lat": 10032132},
            {"$radius": 10},
        ]
    }


**$bbox** - Specifies a bounding box shape with top left and bottom right coordinates. It's value will be an array for 4 coordinates.

    {
        "$bbox": [-20.22, 4, 60.11232, 400]
    }

**$geometry** - Specifies a WKT object. Supported objects are : Point | LineString | Polygon | MultiPoint | MultiLineString | MultiPolygon | GeometryCollection. It's value will nested object with corresponding wkt operator. 
Point will be **$point**, polygons will be **$polygon** and so forth.

    {
        "$geometry": {"$point" : [
            {"$lon": 100 },
            {"$lat": 200 }
         ]
    }

#####ResultSet Operators

ResultSet Operators will be similar to those provided by Hastings:

**$bookmark, $limit, $skip, and $stale.**

In the first version, shoul we support **$format**?

##Usage Scenarios

**1) Give me all the top 25 Chinese or Italian restaurants near current location.**

**Index Creation:**

    {
        "index": {
            "fields": ["location"]
    },
        "name": "restaurant-location",
        "type": "geo"
    }

**Query Syntax:**

    {
        "$selector": {
            "$geoNear": {
                "$geometry": {
                    "$point": [{
                        "$lon": 100
                        }, {
                        "$lat": 200
                    }]
                }
            },
            "$or": [{
                "cuisine": "Chinese"
            }, {
                "cuisine": "Italian"
            }]
        },
        "$limit": 25
    }
