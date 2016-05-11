# Versions
SEARCH_VERSION = 'search.py 2016-05-10T18:23:21+CEST'
DOWNLOAD_VERSION = 'download.py 2016-05-11T11:20:03+CEST'

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
