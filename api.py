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

"""API method and service routing."""

import webapp2

# API methods
from Search.SearchAPI import SearchApi
from Download.DownloadAPI import DownloadApi
from Feedback.FeedbackAPI import FeedbackApi

# Complementary services
from Download.DownloadHandler import DownloadHandler
from Download.CountHandler import CountHandler
from Download.WriteHandler import WriteHandler
from Download.ComposeHandler import ComposeHandler
from Download.CleanupHandler import CleanupHandler

# Query logging
from Querylogger.QueryLogger import QueryLogger

routes = [
    # API methods
    webapp2.Route(r'/api/search', handler=SearchApi),
    webapp2.Route(r'/api/download', handler=DownloadApi),
    webapp2.Route(r'/api/feedback', handler=FeedbackApi),

    # Complementary services
    webapp2.Route(r'/service/download', handler=DownloadHandler),
    webapp2.Route(r'/service/download/count', handler=CountHandler),
    webapp2.Route(r'/service/download/write', handler=WriteHandler),
    webapp2.Route(r'/service/download/compose', handler=ComposeHandler),
    webapp2.Route(r'/service/download/cleanup', handler=CleanupHandler),

    # Query logging
    webapp2.Route(r'/apitracker', handler=QueryLogger),
]

handlers = webapp2.WSGIApplication(routes, debug=True)
