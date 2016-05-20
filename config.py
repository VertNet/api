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

"""API methods configuration file.
"""

# Versions
DOWNLOAD_VERSION = 'download.py 2016-05-13T12:38:30+CEST'
UTIL_VERSION = 'util.py 2016-05-13T12:38:30+CEST'

# Download variables

# limit on documents in a search result: rows per file
SEARCH_CHUNK_SIZE = 1000
# See api_cnt_performance_analysis.pdf at https://goo.gl/xbLIGz
OPTIMUM_CHUNK_SIZE = 500
# limit on the number of files in a single compose request
COMPOSE_FILE_LIMIT = 32
# limit on the number of files in a composition
COMPOSE_OBJECT_LIMIT = 1024
# bucket for temp compositions
TEMP_BUCKET = 'vn-dltest'
# production bucket for downloads
DOWNLOAD_BUCKET = 'vn-downloads2'
FILE_EXTENSION = 'tsv'
