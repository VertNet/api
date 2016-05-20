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

"""Search services.

query - search and return records
query_rec_counter - search records and return count
"""

import json
import logging

from google.appengine.api import namespace_manager
from google.appengine.api import search
from google.appengine.api.search import SortOptions, SortExpression

LAST_UPDATED = '2016-05-20T11:11:48+CEST'


def _get_rec(doc):
    """ Construct an output record from the index document """
    for field in doc.fields:
        if field.name == 'verbatim_record':
            rec = json.loads(field.value)
            rec['rank'] = doc._rank
    return rec


def query(q, limit, index_name='dwc', sort=None, curs=search.Cursor()):
    if not curs:
        curs = search.Cursor()

    if q.startswith('id:'):
        did = q.split(':')[1].strip()
        namespace = namespace_manager.get_namespace()
        results = search.Index(name=index_name, namespace=namespace).get_range(
            start_id=did, limit=1)
        if results:
            recs = map(_get_rec, results)
            logging.info('One result from search.Index()'
                         ' for namespace=%s index_name=%s'
                         ' query=%s\n' % (namespace, index_name, q))
            return recs, None, 1
        else:
            logging.info('No results from search.Index() for namespace=%s'
                         ' index_name=%s query=%s\n' % (
                             namespace, index_name, q)
                         )
            return [], None, 0

    expressions = []

    if sort:
        expressions.append(SortExpression(expression=sort, default_value='z',
                           direction=SortExpression.ASCENDING))
        sort_options = SortOptions(expressions=expressions, limit=limit)
        logging.info('Sort options: %s\n' %
                     (sort_options))

        options = search.QueryOptions(
            limit=limit,
            # See Stucky research, Mar 2014.
            number_found_accuracy=10000,
            cursor=curs,
            sort_options=sort_options)
    else:
        # Always use 10,000 as the value for number_found_accuracy. Based on
        # extensive testing, using this maximum allowed value results in the
        # best count accuracy and incurs only a minor performance penalty.
        options = search.QueryOptions(
            limit=limit,
            # See Stucky research, Mar 2014.
            number_found_accuracy=10000,
            cursor=curs)

    max_retries = 2
    retry_count = 0
    # error = None
    while retry_count < max_retries:
        try:
            query = search.Query(query_string=q, options=options)
            namespace = namespace_manager.get_namespace()
            results = search.Index(
                name=index_name,
                namespace=namespace
            ).search(query)
            if results:
                recs = map(_get_rec, results)
                logging.info('Query: %s results from search.Index() for'
                             ' namespace=%s index_name=%s query=%s'
                             % (q, results.number_found, namespace,
                                index_name))
                return recs, results.cursor, results.number_found
            else:
                logging.info('No results from query %s for namespace=%s'
                             ' index_name=%s\n'
                             % (q, namespace, index_name))
                return [], None, 0
        except Exception, e:
            logging.error('Search failed.\nQUERY:\n'
                          ' %s\nERROR:\n%s\n' % (q, e))
            # error = e
            retry_count += 1
    logging.info('Finally no results from query %s for namespace=%s'
                 ' index_name=%s\n'
                 % (q, namespace, index_name))
    return [], None, 0


def query_rec_counter(q, limit, index_name='dwc',
                      sort=None, curs=search.Cursor()):
    """ Makes a search from curs.
Returns count of records in search, new cursor """
    if not curs:
        curs = search.Cursor()

    if q.startswith('id:'):
        did = q.split(':')[1].strip()
        namespace = namespace_manager.get_namespace()
        results = search.Index(name=index_name, namespace=namespace).get_range(
            start_id=did, limit=1)
        if results:
            recs = len(results.results)
            return recs, None
        else:
            logging.info('No results from search.Index() for namespace=%s'
                         ' index_name=%s query=%s\n' %
                         (namespace, index_name, q))
            return 0, None

    # Always use 10,000 as the value for number_found_accuracy.  Based on
    # extensive testing, using this maximum allowed value results in the best
    # count accuracy and incurs only a minor performance penalty.
    options = search.QueryOptions(
        limit=limit,
        # See Stucky research, Mar 2014.
        number_found_accuracy=10000,
        cursor=curs,
        ids_only=True)

    max_retries = 2
    retry_count = 0
    # error = None
    while retry_count < max_retries:
        try:
            query = search.Query(query_string=q, options=options)
            namespace = namespace_manager.get_namespace()
            results = search.Index(
                name=index_name,
                namespace=namespace
            ).search(query)
            if results:
                recs = len(results.results)
                return recs, results.cursor
            else:
                logging.info('No results from query %s for namespace=%s'
                             'index_name=%s\n'
                             % (q, namespace, index_name))
                return 0, None
        except Exception, e:
            logging.error('Search failed.\nQUERY:\n %s\nERROR:\n%s\n' % (q, e))
            # error = e
            retry_count += 1
    logging.info('Finally no results from query %s for namespace=%s'
                 'index_name=%s\n'
                 % (q, namespace, index_name))
    return 0, None
