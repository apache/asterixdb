import os
import json
import sys

import requests
from requests import ConnectionError, HTTPError

def bootstrap():
    asterix_host = "localhost"
    asterix_port = 19002

    # First, let's get the path to this script, we'll need it to configure the demo.
    base = os.path.dirname(os.path.realpath(__file__))
    print "Running AdmAQL101 from",base

    # First, we bootstrap our request query
    print "Loading TinySocial Dataset..."
    query_statement = open("tinysocial/query.txt").read().split("load")
    for i in range(1,5):
        query_statement[i] = "load" + query_statement[i].replace("FULL_PATH",base + "/tinysocial")
    
    ddl = {
        'ddl': query_statement[0]
    }

    load = {
        "statements" : "use dataverse TinySocial; " + "\n".join(query_statement[1:5])
    }

    http_header = {
        'content-type': 'application/json'
    }

    # Now we run our query
    print "Running query...\n"

    ddl_url = "http://" + asterix_host + ":" + str(asterix_port) + "/ddl"
    update_url = "http://" + asterix_host + ":" + str(asterix_port) + "/update"

    try:
        requests.get(ddl_url, params=ddl)
        response = requests.get(update_url, params=load, headers=http_header)
    except (ConnectionError, HTTPError):
        print "Encountered connection error; stopping execution"
        sys.exit(1)

    return True
