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

"""Download service."""

# Removing dependency on Files API due to its deprecation by Google
import cloudstorage as gcs
from oauth2client.client import GoogleCredentials
from apiclient import discovery
from datetime import datetime

from google.appengine.api import mail
from google.appengine.api import taskqueue
from google.appengine.api import search
from vertnet.service import util as vnutil
from vertnet.service import search as vnsearch
import webapp2
import json
import logging
# import uuid
# import sys




def compose_request(bucketname, filepattern, begin, end):
    """Construct an API Compose dictionary from a bucketname and filepattern.

bucketname - the GCS bucket in which the files will be composed.
    Ex. 'vn-downloads2'
filepattern - the naming pattern for the composed files.
    Ex. 'MyResult-UUID'
begin - the index of the first file in the composition used to grab
    a range of files.
end - begin plus the number of files to put in the composition
    (i.e., end index + 1)
    """
    objectlist = []
    for i in range(begin, end):
        objectdict = {}
        filename = '%s-%s.%s' % (filepattern, i, FILE_EXTENSION)
        objectdict['name'] = filename
        objectlist.append(objectdict)

    composedict = {}
    composedict['sourceObjects'] = objectlist
    dest = {}
    dest['contentType'] = 'text/tab-separated-values'
    dest['bucket'] = bucketname
    composedict['destination'] = dest
    composedict['kind'] = 'storage#composeRequest'
    return composedict


def acl_update_request():
    mbody = {}
    # An acl property cannot be included in the request body
    # if the predefinedAcl parameter is given in the update request
    # the request body is required, and the contentType is required
    # in the request body
    mbody['contentType'] = 'text/tab-separated-values'
    return mbody


class CleanupHandler(webapp2.RequestHandler):
    def post(self):
        filepattern = self.request.get('filepattern')
        compositions = int(self.request.get('compositions'))
        composed_filepattern='%s-cl' % filepattern
        composed_filename='%s.%s' % (filepattern,FILE_EXTENSION)
        total_files_to_compose = int(self.request.get('fileindex'))

        # Get the application default credentials.
        credentials = GoogleCredentials.get_application_default()

        # Construct the service object for interacting with the Cloud Storage API.
        service = discovery.build('storage', 'v1', credentials=credentials)

        if total_files_to_compose>COMPOSE_FILE_LIMIT:
            # Remove the temporary compositions
            for j in range(compositions):
                filename='%s-%s.%s' % (composed_filepattern, j, FILE_EXTENSION)
                req = service.objects().delete(bucket=TEMP_BUCKET, object=filename)

                max_retries = 2
                retry_count = 0
                success = False
                # Try the delete request until successful or until our patience runs out
                while not success and retry_count < max_retries:
                    try:
                        resp = req.execute()
                        success=True
                    except Exception, e:
                        logging.error("Error deleting composed file %s \
attempt %s\nError: %s\nVersion: %s" % (filename, retry_count+1, e, DOWNLOAD_VERSION) )
                        retry_count += 1
#                            raise e

        if total_files_to_compose>1:
            # Remove all of the chunk files used in the composition.
            for j in range(total_files_to_compose):
                filename='%s-%s.%s' % (filepattern, j, FILE_EXTENSION)
                req = service.objects().delete(bucket=TEMP_BUCKET, object=filename)

                max_retries = 2
                retry_count = 0
                success = False
                # Execute the delete request until successful or patience runs out
                while not success and retry_count < max_retries:
                    try:
                        resp = req.execute()
                        success=True
                    except Exception, e:
                        logging.error("Error deleting chunk file %s attempt %s\nError: \
%s\nVersion: %s" % (filename, retry_count+1, e, DOWNLOAD_VERSION) )
                        retry_count += 1
#                        raise e

        # After all else...
        # Delete the temporary composed file from the TEMP_BUCKET
        req = service.objects().delete(bucket=TEMP_BUCKET, object=composed_filename)
        try:
            resp=req.execute()
        except Exception, e:
            logging.error("Error deleting temporary composed file %s \nError: %s\n\
Version: %s" % (filename, e, DOWNLOAD_VERSION) )

        logging.info('Finalized cleaning temporary files from /%s\nVersion: %s' 
            % (TEMP_BUCKET, DOWNLOAD_VERSION) )



api = webapp2.WSGIApplication(routes, debug=True)
