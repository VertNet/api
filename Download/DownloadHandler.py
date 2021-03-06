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
Redirect to appropiate service: download or count
"""

import json
import uuid
import logging
from datetime import datetime

from google.appengine.api import taskqueue
import webapp2

# import search as vnsearch
# import util as vnutil
from config import SEARCH_CHUNK_SIZE
# from config import DOWNLOAD_BUCKET, FILE_EXTENSION

LAST_UPDATED = '2016-05-20T12:37:29+CEST'


class DownloadHandler(webapp2.RequestHandler):
    def _queue(self, q, email, name, latlon, fromapi, source, countonly,
               country, user_agent):
        # Create a base filename for all chunks to be composed for
        # this download
        filepattern = '%s-%s' % (name, uuid.uuid4().hex)
        requesttime = datetime.utcnow().isoformat()

        # Start the download process with the first file having fileindex 0
        # Start the record count at 0
        params = dict(
            q=json.dumps(q), email=email, name=name, filepattern=filepattern,
            latlon=latlon, fileindex=0, reccount=0, requesttime=requesttime,
            source=source, fromapi=fromapi, country=country,
            user_agent=user_agent
        )

        logging.info("About to launch download with following params")
        logging.info(params)

        if countonly is not None and len(countonly) > 0:
            logging.info("Just counts requested")
            taskqueue.add(
                url='/service/download/count',
                params=params,
                queue_name="count"
            )
        else:
            logging.info("Full download requested")
            taskqueue.add(
                url='/service/download/write',
                params=params,
                queue_name="downloadwrite"
            )

    def post(self):
        self.get()

    def get(self):

        # Get params from request
        keywords = self.request.get("keywords")
        count = self.request.get("count")
        email = self.request.get("email")
        DOWNLOAD_VERSION = self.request.get("api")
        source = self.request.get("source")
        latlon = self.request.get("latlon")
        country = self.request.get("country")
        user_agent = self.request.get("user_agent")

        # Optional params
        countonly = self.request.get("countonly")
        name = self.request.get("name")

        # Build query string
        q = ' '.join(json.loads(keywords))

        # Force count to be an integer
        # count is a limit on the number of records to download
        count = int(str(count))

        # Try to send an indicator to the browser if it came from one.
        body = ''
        if countonly is True:
            body = 'Counting results:<br>'
            source = 'CountAPI'
        else:
            body = 'Downloading results:<br>'
        if email is None or len(email) == 0 or email == 'None':
            body += 'ERROR: You must provide an email address.'
        else:
            body += 'File name: %s<br>' % name
            body += 'Email: %s<br>' % email
            body += 'Keywords: %s<br>' % keywords
            body += 'X-AppEngine-CityLatLong: %s<br>' % latlon
            body += 'X-AppEngine-Country: %s<br>' % country
            body += 'User-Agent: %s<br>' % user_agent
            body += 'Source: %s<br>' % source
            body += 'API: %s<br>' % DOWNLOAD_VERSION
            body += 'Request headers: %s<br>' % self.request.headers

        self.response.out.write(body)
        logging.info('API download request. Source: %s Count: %s \
Keywords: %s Email: %s Name: %s LatLon: %s User-Agent: %s\nVersion: %s'
                     % (source, count, keywords, email, name,
                        latlon, user_agent, DOWNLOAD_VERSION))
        if email is None or len(email) == 0:
            return

        if count == 0 or count > SEARCH_CHUNK_SIZE:
            # The results are larger than SEARCH_CHUNK_SIZE, compose a file
            self._queue(q, email, name, latlon, DOWNLOAD_VERSION, source,
                        countonly, country, user_agent)

# # WITH CURRENT API DOWNLOADS, THIS 'else' WILL NEVER HAPPEN.
#
# # NOTE: This "could" happen if a more efficient 'count' method is implemented
# # since that would allow for fast calculation of record counts and could
# # redirect here if count <= 1K
#
#
#
#         else:
#             # The results are smaller than SEARCH_CHUNK_SIZE,
#             # download directly and make
#             # a copy of the file in the download bucket
#             filename = str('%s.txt' % name)
#             self.response.headers['Content-Type']="text/tab-separated-values"
#             self.response.headers['Content-Disposition'] = \
#                 "attachment; filename=%s" % filename
#             records, cursor, count, query_version = vnsearch.query(q, count)

#             # Build dictionary for search counts
#             res_counts = vnutil.search_resource_counts(records)

#             # Write the header for the output file
#             data = '%s\n%s' % (vnutil.download_header(),
#                                vnutil._get_tsv_chunk(records))
#             self.response.out.write(data)

#             # Write single chunk to file in DOWNLOAD_BUCKET
#             filepattern = '%s-%s' % (name, uuid.uuid4().hex)
#             filename = '/%s/%s.%s' % (DOWNLOAD_BUCKET, filepattern,
#                                       FILE_EXTENSION)

#             # Parameters for the coming apitracker taskqueue
#             apitracker_params = dict(
#                 api_version=fromapi, count=len(records), download=filename,
#                 downloader=email, error=None, latlon=latlon,
#                 matching_records=len(records), query=q,
#                 query_version=query_version, request_source=source,
#                 response_records=len(records),
#                 res_counts=json.dumps(res_counts), type='download')

#             max_retries = 2
#             retry_count = 0
#             success = False
#             while not success and retry_count < max_retries:
#                 try:
#                     with gcs.open(filename, 'w',
#                                   content_type='text/tab-separated-values',
#                                   options={'x-goog-acl': 'public-read'}) as f:
#                         f.write(data)
#                         success = True
#                        logging.info('Sending small res_counts to tracker: %s'
#                            % res_counts )
#                         taskqueue.add(url='/apitracker',
#                                       params=apitracker_params,
#                                       queue_name="apitracker")
#                 except Exception, e:
#                     logging.error(
#                         "Error writing small result set to %s.\nError: %s \n\
#                          Version: %s" % (filename, e, DOWNLOAD_VERSION))
#                     retry_count += 1
# #                    raise e
