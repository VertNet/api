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
# along with VertNet.  If not, see:     http://www.gnu.org/licenses

"""Download service.

Delete all temp chunks and files

"""

import logging

from oauth2client.client import GoogleCredentials
from apiclient import discovery
import webapp2

from config import FILE_EXTENSION, COMPOSE_FILE_LIMIT, TEMP_BUCKET

LAST_UPDATED = '2016-05-20T12:37:29+CEST'


class CleanupHandler(webapp2.RequestHandler):
    def post(self):
        filepattern = self.request.get('filepattern')
        DOWNLOAD_VERSION = self.request.get('api')
        compositions = int(self.request.get('compositions'))
        composed_filepattern = '%s-cl' % filepattern
        composed_filename = '%s.%s' % (filepattern, FILE_EXTENSION)
        total_files_to_compose = int(self.request.get('fileindex'))

        # Get the application default credentials.
        credentials = GoogleCredentials.get_application_default()

        # Construct the service object for interacting with the Storage API.
        service = discovery.build('storage', 'v1', credentials=credentials)

        if total_files_to_compose > COMPOSE_FILE_LIMIT:
            # Remove the temporary compositions
            for j in range(compositions):
                filename = '%s-%s.%s' % (composed_filepattern, j,
                                         FILE_EXTENSION)
                req = service.objects().delete(bucket=TEMP_BUCKET,
                                               object=filename)

                max_retries = 2
                retry_count = 0
                success = False
                # Try the delete request until successful or patience runs out
                while not success and retry_count < max_retries:
                    try:
                        req.execute()
                        success = True
                    except Exception, e:
                        logging.error("Error deleting composed file %s "
                                      "attempt %s\nError: %s\nVersion: %s"
                                      % (filename, retry_count+1, e,
                                         DOWNLOAD_VERSION))
                        retry_count += 1
#                            raise e

        if total_files_to_compose > 1:
            # Remove all of the chunk files used in the composition.
            for j in range(total_files_to_compose):
                filename = '%s-%s.%s' % (filepattern, j, FILE_EXTENSION)
                req = service.objects().delete(bucket=TEMP_BUCKET,
                                               object=filename)

                max_retries = 2
                retry_count = 0
                success = False
                # Execute the delete until successful or patience runs out
                while not success and retry_count < max_retries:
                    try:
                        req.execute()
                        success = True
                    except Exception, e:
                        logging.error("Error deleting chunk file %s attempt %s"
                                      "\nError: %s\nVersion: %s"
                                      % (filename, retry_count+1, e,
                                         DOWNLOAD_VERSION))
                        retry_count += 1
#                        raise e

        # After all else...
        # Delete the temporary composed file from the TEMP_BUCKET
        req = service.objects().delete(bucket=TEMP_BUCKET,
                                       object=composed_filename)
        try:
            req.execute()
        except Exception, e:
            logging.error("Error deleting temporary composed file %s \nError: "
                          "%s\nVersion: %s" % (filename, e, DOWNLOAD_VERSION))

        logging.info('Finalized cleaning temporary files from /%s\nVersion: %s'
                     % (TEMP_BUCKET, DOWNLOAD_VERSION))
