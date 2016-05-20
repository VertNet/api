# This file is part of VertNet: https://github.com/VertNet/webapp
#
# VertNet is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# VertNet is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with VertNet.  If not, see: http://www.gnu.org/licenses

"""Download API.

Initialize download service.

Endpoint:
  http://api-module.vertnet-portal.appspot.com/api/download
Args:
  q: query object. E.g. "mappable:1 institutioncode:kstc"
  e: email to send notification to
  n: name of the download file
  c: counts only, return only number of records and not records themselves
  o: origin, indicates query origin - api, portal...
"""

import os
import json
import logging

from google.appengine.api import taskqueue
import webapp2

LAST_UPDATED = '2016-05-20T12:37:29+CEST'
DOWNLOAD_VERSION = 'download 2016-05-20T12:37:29+CEST'

IS_DEV = os.environ.get('SERVER_SOFTWARE', '').startswith('Development')
if IS_DEV:
    QUEUE_NAME = 'default'
else:
    QUEUE_NAME = 'download'


class DownloadApi(webapp2.RequestHandler):
    """Example download request:
        http://api.vertnet-portal.appspot.com/api/download?q= \
        {"q":"mappable:1 institutioncode:kstc","n":"kstctestresults.txt", \
        "e":"you@gmail.com"}
        Example count request:
        http://api.vertnet-portal.appspot.com/api/download?q= \
        {"q":"mappable:1 institutioncode:kstc","n":"kstctestresults.txt", \
        "e":"you@gmail.com", "c":"True"}
    """
    def __init__(self, request, response):
        self.latlon = request.headers.get('X-AppEngine-CityLatLong')
        self.country = request.headers.get('X-AppEngine-Country')
        self.user_agent = request.headers.get('User-Agent')
        self.initialize(request, response)

    def post(self):
        self.get()

    def get(self):
        # Receive the download request and redirect it to the download URL
        logging.info('Version: %s\nAPI download request: %s'
                     % (DOWNLOAD_VERSION, self.request))
        request = json.loads(self.request.get('q'))
        q, e, n, countonly, source = map(request.get,
                                         ['q', 'e', 'n', 'c', 'o'])
        keywords = q.split()

        # Apply default value to source parameter
        if not source or source is None or source == "" or source == "None":
            source = 'DownloadAPI'

        params = {
            "keywords": json.dumps(keywords),
            "count": 0,
            "email": e,
            "api": DOWNLOAD_VERSION,
            "source": source,
            "latlon": self.latlon,
            "country": self.country,
            "user_agent": self.user_agent
        }

        if countonly is not None:
            params["countonly"] = True

        else:
            params["name"] = n

        taskqueue.add(url="/service/download",
                      params=params,
                      queue_name=QUEUE_NAME)

        resp = {
            "result": "success",
            "file_name": n,
            "email": e,
            "query": q,
            "api_version": DOWNLOAD_VERSION,
            "source": source
        }
        self.response.headers.add_header("Access-Control-Allow-Origin", "*")
        self.response.headers['Content-Type'] = "application/json"
        self.response.write(json.dumps(resp))
