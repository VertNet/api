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

import json
import logging
from datetime import datetime

from google.appengine.api import search, taskqueue
import webapp2

import search as vnsearch
import util as vnutil

API_VERSION = 'api.py 2016-05-10T18:23:51+CEST'


class SearchApi(webapp2.RequestHandler):
    def __init__(self, request, response):
        self.cityLatLong = request.headers.get('X-AppEngine-CityLatLong')
        logging.info('Init Request headers: %s\nVersion: %s' %
                     (request.headers, API_VERSION))
        self.initialize(request, response)

    def post(self):
        self.get()

    def get(self):
        logging.info('API search request: %s\nVersion: %s' % (self.request,
                                                              API_VERSION))
        # TODO: Add clause to catch missing 'q' error
        request = json.loads(self.request.get('q'))
        q, c, limit = map(request.get, ['q', 'c', 'l'])

        # Set the limit to 400 by default.  This value is based on the results
        # of substantial performance testing.
        if not limit:
            limit = 400
        if limit > 1000:
            # 1000 is the maximum value allowed by Google.
            limit = 1000
        if limit < 0:
            limit = 1

        curs = None
        if c:
            curs = search.Cursor(web_safe_string=c)
        else:
            curs = search.Cursor()

        result = vnsearch.query(q, limit, 'dwc', sort=None, curs=curs)
        response = None

        if len(result) == 4:
            recs, cursor, count, query_version = result
            if not c:
                type = 'query'
                # query_count = count
            else:
                type = 'query-view'
                # query_count = limit
            if cursor:
                cursor = cursor.web_safe_string

            # If count > 10,000, do not return the actual value of count
            # because it will be unreliable.  Extensive testing revealed that
            # even for relatively small queries (>10,000 but <30,000 records),
            # it can be in error by one or more orders of magnitude.
            if count > 10000:
                count = '>10000'

            d = datetime.utcnow()

            # Process dynamicProperties JSON formatting
            for r in recs:
                if 'dynamicproperties' in r:
                    r['dynamicproperties'] = vnutil.format_json(
                        r['dynamicproperties']
                    )

            response = json.dumps(dict(
                recs=recs, cursor=cursor, matching_records=count,
                limit=limit, response_records=len(recs),
                api_version=API_VERSION, query_version=query_version,
                request_date=d.isoformat(), request_origin=self.cityLatLong
            ))

            logging.info('API search recs: %s\nVersion: %s' % (recs,
                                                               API_VERSION))
            res_counts = vnutil.search_resource_counts(recs)

            params = dict(
                api_version=API_VERSION, count=len(recs),
                latlon=self.cityLatLong, matching_records=count, query=q,
                query_version=query_version, request_source='SearchAPI',
                response_records=len(recs), res_counts=json.dumps(res_counts),
                type=type
            )
            taskqueue.add(
                url='/apitracker',
                params=params,
                queue_name="apitracker"
            )
        else:
            error = result[0].__class__.__name__
            params = dict(
                error=error,
                query=q,
                type='query',
                latlon=self.cityLatLong
            )
            taskqueue.add(
                url='/apitracker',
                params=params,
                queue_name="apitracker"
            )
            self.response.clear()
            message = 'Please try again. Error: %s' % error
            self.response.set_status(500, message=message)
            response = message

        self.response.out.headers['Content-Type'] = 'application/json'
        self.response.headers['charset'] = 'utf-8'
        self.response.out.write(response)