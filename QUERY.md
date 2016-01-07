# Cloudant Query Geospatial

## Overview


This is an ongoing requirements document describing the user scenarios and syntax for adding geospatial support to Cloudant Query.

## Index Creation

We will continue using the Cloudant Query index creation structure. The simplest format will be as follows:

    {
        "index": {
            "fields": ["geometry", "location"]
    },
        "name" : "foo-index",
        "type" : "geo"
    }

This essentially indexes GeoJson objects referenced by the fields "geometry" and "location". At the Cloudant Query indexing level, we ensure that the object is valid GeoJson.  

A possible alternative could be a stricter definition such as:

    {
        "index": {
            "fields": [
                {"geometry": "Point"},  
                {"location" : "Polygon"]
    },
        "name" : "foo-index",
        "type" : "geo"
    }

In this case, Cloudant Query will ensure that the "type" field in the GeoJson object matches the type provided by the user.

## Query Syntax