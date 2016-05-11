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
from DownloadAPI import DownloadApi

from DownloadHandler import DownloadHandler
from CountHandler import CountHandler
# from WriteHandler import WriteHandler
# from ComposeHandler import ComposeHandler

# from CleanupHandler import CleanupHandler

VERSION = '2016-05-10T18:06:45+CEST'


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
    webapp2.Route(r'/api/download', handler=DownloadApi),
    # webapp2.Route(r'/api/feedback', handler=FeedbackApi),

    webapp2.Route(r'/service/download', handler=DownloadHandler),
    webapp2.Route(r'/service/download/count', handler=CountHandler),
    # webapp2.Route(r'/service/download/write', handler=WriteHandler),
    # webapp2.Route(r'/service/download/compose', handler=ComposeHandler),
    # webapp2.Route(r'/service/download/cleanup', handler=CleanupHandler),
]

handlers = webapp2.WSGIApplication(routes, debug=True)
