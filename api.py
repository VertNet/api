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

# from google.appengine.api import search, taskqueue
# from vertnet.service import search as vnsearch
# from vertnet.service import util as vnutil
# from datetime import datetime, date, time

# import json
# import logging
# import urllib
import webapp2

from SearchAPI import SearchApi

VERSION = '2016-05-10T18:06:45+CEST'

# class DownloadApi(webapp2.RequestHandler):
#     """Example download request:
#         http://api.vertnet-portal.appspot.com/api/download?q= \
#         {"q":"mappable:1 institutioncode:kstc","n":"kstctestresults.txt", \
#         "e":"you@gmail.com"}
#         Example count request:
#         http://api.vertnet-portal.appspot.com/api/download?q= \
#         {"q":"mappable:1 institutioncode:kstc","n":"kstctestresults.txt", \
#         "e":"you@gmail.com", "c":"True"}
#     """
#     def __init__(self, request, response):
#         self.cityLatLong = request.headers.get('X-AppEngine-CityLatLong')
#         self.initialize(request, response)

#     def post(self):
#         self.get()

#     def get(self):
#         # Receive the download request and redirect it to the download service URL
# #        logging.info('Version: %s\nAPI download request: %s' 
# #            % (API_VERSION, self.request) )
#         request = json.loads(self.request.get('q'))
#         q, e, n, countonly = map(request.get, ['q', 'e', 'n', 'c'])
#         keywords = q.split()
#         if countonly is not None:
#             params = urllib.urlencode(dict(keywords=json.dumps(keywords), count=0,
#                 email=e, countonly=True, api=API_VERSION))
#         else:
#             params = urllib.urlencode(dict(keywords=json.dumps(keywords), count=0, 
#                 email=e, name=n, api=API_VERSION))
#         url = '/service/download?%s' % params
#         self.redirect(url)

# class FeedbackApi(webapp2.RequestHandler):
#     def post(self):
#         self.get()

#     def get(self):
# #        logging.info('API feedback request: %s\nVersion: %s' 
# #            % (self.request, API_VERSION) )
#         request = json.loads(self.request.get('q'))
#         url = '/api/github/issue/create?q=%s' % request
#         self.redirect(url)

routes = [
    webapp2.Route(r'/api/search', handler=SearchApi),
    # webapp2.Route(r'/api/download', handler=DownloadApi),
    # webapp2.Route(r'/api/feedback', handler=FeedbackApi)
]

handlers = webapp2.WSGIApplication(routes, debug=True)
