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

"""Download service.

Get parameters from request
Get record count from vnsearch.query_rec_counter
Send email to user with result
"""

import json
import logging
from datetime import datetime

from google.appengine.api import search, taskqueue, mail
import webapp2

import search as vnsearch
from config import OPTIMUM_CHUNK_SIZE, DOWNLOAD_VERSION


class CountHandler(webapp2.RequestHandler):
    def post(self):

        # Get parameters from request
        q = json.loads(self.request.get('q'))
        latlon = self.request.get('latlon')
        requesttime = self.request.get('requesttime')
        reccount = int(self.request.get('reccount'))
        fromapi = self.request.get('fromapi')
        source = self.request.get('source')
        cursor = self.request.get('cursor')
        email = self.request.get('email')

        if cursor:
            curs = search.Cursor(web_safe_string=cursor)
        else:
            curs = ''

        records, next_cursor, query_version = vnsearch.query_rec_counter(
            q, OPTIMUM_CHUNK_SIZE, curs=curs
        )
        logging.info("Got %d records this round" % records)

        # Update the total number of records retrieved
        reccount = reccount+records

        if next_cursor:
            curs = next_cursor.web_safe_string
        else:
            curs = None

        if curs:
            countparams = dict(
                q=self.request.get('q'), cursor=curs, reccount=reccount,
                requesttime=requesttime, fromapi=fromapi, source=source,
                latlon=latlon, email=email)

            logging.info('Record counter. Count: %s Email: %s Query: %s'
                         ' Cursor: %s Version: %s' %
                         (reccount, email, q, next_cursor, DOWNLOAD_VERSION))
            # Keep counting
            taskqueue.add(
                url='/service/download/count',
                params=countparams,
                queue_name="test"
            )

        else:
            # Finished counting. Log the results and send email.
            # logging.info('Finished counting. Record total: %s Email: %s'
            #              ' Query %s Cursor: %s\nVersion: %s' %
            #              (reccount, email, q, next_cursor, DOWNLOAD_VERSION))

            # apitracker_params = dict(
            #     api_version=fromapi, count=reccount, query=q, latlon=latlon,
            #     query_version=query_version, request_source=source,
            #     type='count', downloader=email
            # )
            #
            # taskqueue.add(
            #     url='/apitracker',
            #     params=apitracker_params,
            #     queue_name="apitracker"
            # )

            resulttime = datetime.utcnow().isoformat()
            mail.send_mail(
                sender="VertNet Counts <vertnetinfo@vertnet.org>",
                to=email,
                subject="Your VertNet count is ready!",
                body="""Your query found %s matching records.
Query: %s
Request submitted: %s
Request fulfilled: %s
""" % (reccount, q, requesttime, resulttime))

            logging.info("Successfully sent mail to user")
