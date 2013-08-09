from twisted.internet import reactor
from twisted.web import static, proxy, server

import os
import json
import sys

from lib.requests.requests import requests

asterix_host = "localhost"
asterix_port = 19002

# First, let's get the path to this script, we'll need it to configure the demo.
base = os.path.dirname(os.path.realpath(__file__))
print "Running Black Cherry from",base

# First, we bootstrap our request query
print "Preparing DDL..\n"
query_statement = open("src/data/query.txt").read().split("load")
query_statement[1] = "load" + query_statement[1].replace("FULL_PATH",base + "/src/data")

ddl = {
    'ddl': query_statement[0]
}

load = {
    "statements" : "use dataverse twitter; " + query_statement[1]
}

http_header = {
    'content-type': 'application/json'
}

# Now we run our query
print "Running query...\n"

ddl_url = "http://" + asterix_host + ":" + str(asterix_port) + "/ddl"
update_url = "http://" + asterix_host + ":" + str(asterix_port) + "/update"
requests.get(ddl_url, params=ddl)
response = requests.get(update_url, params=load, headers=http_header)
if (response.status_code != 200):
    print "Oh no, something went wrong! Could not load dataset. Is Asterix running on this host?"
    sys.exit()

# Now we obtain the path of this simple python server, and appends src to it (where Black Cherry lives)
path = os.path.dirname(os.path.realpath(__file__)) + "/src"
root = static.File(path)     # will be served under '/'

# reverse proxy, served under '/asterix', with same endpoints as Asterix REST API
# http://test.company.com/service1/v1/JsonService -> becomes http://localhost/svc/service1/v1/JsonService
root.putChild('asterix', proxy.ReverseProxyResource(asterix_host, asterix_port, ''))

print "Please open http://localhost:8080/cherry.html"

# magic
site = server.Site(root)
reactor.listenTCP(8080, site)
reactor.run()
