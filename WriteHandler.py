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

Get chunk of records via vnsearch.search
Count results and update any existing count
Build TSV chunk and store in temp GCS bucket
If more chunks to process, repeat
Otherwise, log in apitracker and go to 'compose'
"""

import json
import logging

import cloudstorage as gcs
from google.appengine.api import search, taskqueue
import webapp2

import search as vnsearch
import util as vnutil
from config import DOWNLOAD_VERSION, TEMP_BUCKET, FILE_EXTENSION, \
    SEARCH_CHUNK_SIZE, DOWNLOAD_BUCKET, COMPOSE_OBJECT_LIMIT


class WriteHandler(webapp2.RequestHandler):
    def post(self):

        # Get params from request
        q, email, name, latlon = map(self.request.get,
                                     ['q', 'email', 'name', 'latlon'])
        q = json.loads(q)
        requesttime = self.request.get('requesttime')
        filepattern = self.request.get('filepattern')
        fileindex = int(self.request.get('fileindex'))
        reccount = int(self.request.get('reccount'))
        fromapi = self.request.get('fromapi')
        source = self.request.get('source')
        filename = '/%s/%s-%s.%s' % (TEMP_BUCKET,
                                     filepattern,
                                     fileindex,
                                     FILE_EXTENSION)
        cursor = self.request.get('cursor')

        try:
            total_res_counts = json.loads(self.request.get('res_counts'))
        except:
            total_res_counts = {}

        if cursor:
            curs = search.Cursor(web_safe_string=cursor)
        else:
            curs = None

        # Write single chunk to file, GCS does not support append
        records, next_cursor, count, query_version = \
            vnsearch.query(q, SEARCH_CHUNK_SIZE, curs=curs)
        # Build dict for search counts
        res_counts = vnutil.search_resource_counts(records, total_res_counts)

        # Now merge the two dictionaries, summing counts
        if total_res_counts is None or len(total_res_counts) == 0:
            total_res_counts = res_counts
        else:
            for r in res_counts:
                try:
                    count = total_res_counts[r]
                    total_res_counts[r] = count+res_counts[r]
                except:
                    total_res_counts[r] = res_counts[r]

        # Update the total number of records retrieved
        reccount = reccount+len(records)

        # Make a chunk to write to a file
        chunk = '%s\n' % vnutil._get_tsv_chunk(records)

        if fileindex == 0 and not next_cursor:
            # This is a query with fewer than SEARCH_CHUNK_SIZE results
            filename = '/%s/%s.%s' % (TEMP_BUCKET, filepattern, FILE_EXTENSION)

        max_retries = 2
        retry_count = 0
        success = False
        while not success and retry_count < max_retries:
            try:
                with gcs.open(filename, 'w',
                              content_type='text/tab-separated-values',
                              options={'x-goog-acl': 'public-read'}) as f:
                    if fileindex == 0:
                        f.write('%s\n' % vnutil.download_header())
                    f.write(chunk)
                    success = True
                    logging.info('Download chunk saved to %s: Total %s records'
                                 ' Has next cursor: %s \nVersion: %s'
                                 % (filename, reccount,
                                    next_cursor is not None, DOWNLOAD_VERSION))
            except Exception, e:
                logging.error("Error writing chunk to FILE: %s for\nQUERY: %s"
                              "Error: %s\nVersion: %s" %
                              (filename, q, e, DOWNLOAD_VERSION))
                retry_count += 1
#                raise e

        # Queue up next chunk or current chunk if failed to write
        if not success:
            next_cursor = curs
        if next_cursor:
            curs = next_cursor.web_safe_string
        else:
            curs = ''

        # Parameters for the coming apitracker taskqueue
        finalfilename = '/%s/%s.%s' % (DOWNLOAD_BUCKET, filepattern,
                                       FILE_EXTENSION)

        if curs:
            fileindex = fileindex + 1

            if fileindex > COMPOSE_OBJECT_LIMIT:
                # Opt not to support downloads of more than
                # COMPOSE_OBJECT_LIMIT*SEARCH_CHUNK_SIZE records
                # Stop composing results at this limit.

                apitracker_params = dict(
                    api_version=fromapi, count=reccount,
                    download=finalfilename, downloader=email, error=None,
                    latlon=latlon, matching_records=reccount, query=q,
                    query_version=query_version, request_source=source,
                    response_records=len(records),
                    res_counts=json.dumps(total_res_counts), type='download'
                )

                composeparams = dict(
                    email=email, name=name, filepattern=filepattern, q=q,
                    fileindex=fileindex, reccount=reccount,
                    requesttime=requesttime
                )

                # Log the download
                # taskqueue.add(
                #     url='/apitracker', params=apitracker_params,
                #     queue_name="apitracker"
                # )

                taskqueue.add(
                    url='/service/download/compose', params=composeparams,
                    queue_name="compose"
                )
            else:
                writeparams = dict(
                    q=self.request.get('q'), email=email, name=name,
                    filepattern=filepattern, latlon=latlon, cursor=curs,
                    fileindex=fileindex,
                    res_counts=json.dumps(total_res_counts), reccount=reccount,
                    requesttime=requesttime, fromapi=fromapi, source=source
                )

#                logging.info('Sending total_res_counts to write again: %s'
#                    % total_res_counts)
                # Keep writing search chunks to files
                taskqueue.add(
                    url='/service/download/write', params=writeparams,
                    queue_name="downloadwrite"
                )

        else:
            # Log the download
            apitracker_params = dict(
                api_version=fromapi, count=reccount, download=finalfilename,
                downloader=email, error=None, latlon=latlon,
                matching_records=reccount, query=q,
                query_version=query_version, request_source=source,
                response_records=len(records),
                res_counts=json.dumps(total_res_counts), type='download'
            )

            composeparams = dict(
                email=email, name=name, filepattern=filepattern, q=q,
                fileindex=fileindex, reccount=reccount,
                requesttime=requesttime
            )

            # taskqueue.add(
            #     url='/apitracker', params=apitracker_params,
            #     queue_name="apitracker"
            # )

            # Finalize and email.
            taskqueue.add(
                url='/service/download/compose', params=composeparams,
                queue_name="compose"
            )
