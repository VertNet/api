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





class ComposeHandler(webapp2.RequestHandler):
    def post(self):
        q, email, name, filepattern, latlon = map(self.request.get, 
            ['q', 'email', 'name', 'filepattern', 'latlon'])
        requesttime = self.request.get('requesttime')
        composed_filepattern='%s-cl' % filepattern
        reccount = self.request.get('reccount')
        total_files_to_compose = int(self.request.get('fileindex'))+1
        compositions=total_files_to_compose

        # Get the application default credentials.
        credentials = GoogleCredentials.get_application_default()

        # Construct the service object for interacting with the Cloud Storage API.
        service = discovery.build('storage', 'v1', credentials=credentials)

        # Compose chunks into composite objects until there is one composite object
        # Then remove all the chunks and return a URL to the composite
        # Only 32 objects can be composed at once, limit 1024 in a composition of 
        # compositions. Thus, a composition of compositions is sufficient for the worst
        # case scenario.
        # See https://cloud.google.com/storage/docs/composite-objects#_Compose
        # See https://cloud.google.com/storage/docs/json_api/v1/objects/compose#examples

        if total_files_to_compose>COMPOSE_FILE_LIMIT:
            # Need to do a composition of compositions
            # Compose first round as sets of COMPOSE_FILE_LIMIT or fewer files

            compositions=0
            begin=0

            while begin<total_files_to_compose:
                # As long as there are files to compose, compose them in sets of up to
                # COMPOSE_FILE_LIMIT files.
                end=total_files_to_compose
                if end-begin>COMPOSE_FILE_LIMIT:
                    end=begin+COMPOSE_FILE_LIMIT

                composed_filename='%s-%s.%s' % (composed_filepattern, compositions, 
                    FILE_EXTENSION)
                mbody=compose_request(TEMP_BUCKET, filepattern, begin, end)
                req = service.objects().compose(
                    destinationBucket=TEMP_BUCKET,
                    destinationObject=composed_filename,
                    destinationPredefinedAcl='publicRead',
                    body=mbody)

                try:
#                        resp = req.execute()
                        # Don't worry about getting the response. Just execute the request.
                    req.execute()
                except Exception, e:
                    # There is a timeout fetching the url for the response. This
                    # tends to happen in compose requests for LARGE downloads.
                    logging.warning("Deadline exceeded error (not to worry) \
composing file: %s Query: %s Email: %s Error: %s\nVersion: %s" 
                        % (composed_filename, q, email, e, DOWNLOAD_VERSION) )
                    pass
                        
                begin=begin+COMPOSE_FILE_LIMIT
                compositions=compositions+1

        composed_filename='%s.%s' % (filepattern,FILE_EXTENSION)
        if compositions==1:
            logging.info('%s requires no composition.\nVersion: %s' 
                % (composed_filename, DOWNLOAD_VERSION) )
        else:
            # Compose remaining files
            fp = filepattern
            if total_files_to_compose>COMPOSE_FILE_LIMIT:
                # If files were composed, compose them further
                fp = composed_filepattern
            mbody=compose_request(TEMP_BUCKET, fp, 0, compositions)
#            logging.info('Composing %s files into %s\nmbody:\n%s 
#                \nVersion: %s' % (compositions,composed_filename, mbody, \
#                DOWNLOAD_VERSION) )
            req = service.objects().compose(
                destinationBucket=TEMP_BUCKET,
                destinationObject=composed_filename,
                destinationPredefinedAcl='publicRead',
                body=mbody)
            try:
                # Don't worry about getting the response. Just execute the request.
                req.execute()
            except Exception, e:
                # There is a timeout fetching the url for the response. This
                # tends to happen in compose requests for LARGE downloads when composing
                # already composed files. Just log it and hope for the best.
                logging.warning("Deadline exceeded error (not to worry) composing \
file: %s Error: %s\nVersion: %s" % (composed_filename, e, DOWNLOAD_VERSION) )
                pass
#                    logging.error("Error composing file: %s Error: %s\nVersion: %s" 
#                        % (composed_filename, e, DOWNLOAD_VERSION) )
#                retry_count += 1
                
        # Now, can we zip the final result?
        # Not directly with GCS. It would have to be done using gsutil in Google 
        # Compute Engine

        # Copy the file from temporary storage bucket to the download bucket
        src = '/%s/%s' % (TEMP_BUCKET, composed_filename)
        dest = '/%s/%s' % (DOWNLOAD_BUCKET, composed_filename)
        try:
            gcs.copy2(src, dest)
        except Exception, e:
            logging.error("Error copying %s to %s \nError: %s\
Version: %s" % (src, dest, e, DOWNLOAD_VERSION) )

        # Change the ACL so that the download file is publicly readable.
        mbody=acl_update_request()
#        logging.info('Requesting update for /%s/%s\nmbody%s \
#            \nVersion: %s' % (DOWNLOAD_BUCKET,composed_filename, mbody, 
#            DOWNLOAD_VERSION) )
        req = service.objects().update(
                bucket=DOWNLOAD_BUCKET,
                object=composed_filename,
                predefinedAcl='publicRead',
                body=mbody)
        resp=req.execute()

        resulttime=datetime.utcnow().isoformat()
        if total_files_to_compose>COMPOSE_OBJECT_LIMIT:
            mail.send_mail(sender="VertNet Downloads <vertnetinfo@vertnet.org>", 
                to=email, subject="Your truncated VertNet download is ready!",
                body="""
Your VertNet download file is now available for a limited time at 
https://storage.googleapis.com/%s/%s.\n
The results in this file are not complete based on your query\n
%s\n
The number of records in the results of your query exceeded the limit. This is a limit 
based on the VertNet architecture. If you need more results than what you received, 
consider making multiple queries with distinct criteria, such as before and after a 
given year.
If you need very large data sets, such as all Mammals, these are pre-packaged 
periodically and accessible for download. Please contact vertnetinfo@vertnet.org for 
more information.\n
Matching records: %s\nRequest submitted: %s\nRequest fulfilled: %s""" 
                    % (DOWNLOAD_BUCKET, composed_filename, q, reccount, requesttime, 
                    resulttime))
        else:
            mail.send_mail(sender="VertNet Downloads <vertnetinfo@vertnet.org>", 
                to=email, subject="Your VertNet download is ready!",
                body="""
Your VertNet download file is now available for a limited time at 
https://storage.googleapis.com/%s/%s.\n
Query: %s\nMatching records: %s\nRequest submitted: %s\nRequest fulfilled: %s""" 
                    % (DOWNLOAD_BUCKET, composed_filename, q, reccount, requesttime, 
                    resulttime))

        logging.info('Finalized writing /%s/%s\nVersion: %s' 
            % (DOWNLOAD_BUCKET, composed_filename, DOWNLOAD_VERSION) )

        cleanupparams = dict(filepattern=filepattern, fileindex=total_files_to_compose, 
            compositions=compositions)
        taskqueue.add(url='/service/download/cleanup', params=cleanupparams, 
            queue_name="cleanup")


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
