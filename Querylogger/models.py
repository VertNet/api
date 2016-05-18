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

from google.appengine.ext import ndb


class ResourceLogEntry(ndb.Model):
    """Model for the count of records for a particular resource on a
particular query event. Id is gbifdatasetid, ancestor is LogEntry id."""
    count = ndb.IntegerProperty(required=True)


class LogEntry(ndb.Model):
    """Model for log entry. Id automatically assigned."""

    # Query event metadata
    lat = ndb.FloatProperty()
    lon = ndb.FloatProperty()
    country = ndb.StringProperty()
    created_at = ndb.DateTimeProperty(auto_now_add=True)
    updated_at = ndb.DateTimeProperty(auto_now=True)

    # Query metadata
    query = ndb.StringProperty(required=True)
    client = ndb.StringProperty(required=True)
    type = ndb.StringProperty()
    api_version = ndb.StringProperty(required=True)
    request_source = ndb.StringProperty(required=True)
    query_version = ndb.StringProperty(required=True)

    # Query result metadata
    error = ndb.StringProperty()
    count = ndb.IntegerProperty()
    matching_records = ndb.IntegerProperty()
    response_records = ndb.IntegerProperty()
    results_by_resource = ndb.StructuredProperty(ResourceLogEntry,
                                                 repeated=True)

    # Download-specific metadata
    downloader = ndb.StringProperty()
    download = ndb.StringProperty()

    # Kept for CartoDB compatibility
    cartodb_id = ndb.IntegerProperty()
    the_geom = ndb.StringProperty()
    the_geom_webmercator = ndb.StringProperty()
