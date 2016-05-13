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

import webapp2


class FeedbackApi(webapp2.RequestHandler):
    def post(self):
        self.get()

    def get(self):
        # logging.info('API feedback request: %s\nVersion: %s'
        #     % (self.request, API_VERSION))
        request = json.loads(self.request.get('q'))
        url = '/api/github/issue/create?q=%s' % request
        self.redirect(url)
