import json
import logging
import threading
import time
from datetime import datetime

import requests
import solr
from shapely import wkt

SOLR_CON_LOCK = threading.Lock()
thread_local = threading.local()


class SolrProxy(object):
    def __init__(self, config):
        self.solrUrl = config.get("solr", "host")
        self.solrCore = config.get("solr", "core")
        self.logger = logging.getLogger('nexus')

        with SOLR_CON_LOCK:
            solrcon = getattr(thread_local, 'solrcon', None)
            if solrcon is None:
                solrcon = solr.Solr('http://%s/solr/%s' % (self.solrUrl, self.solrCore), debug=False)
                thread_local.solrcon = solrcon

            self.solrcon = solrcon

    def find_tile_by_id(self, tile_id):

        search = 'id:%s' % tile_id

        params = {
            'rows': 1
        }

        results, start, found = self.do_query(*(search, None, None, True, None), **params)

        assert len(results) == 1, "Found %s results, expected exactly 1" % len(results)
        return [results[0]]

    def find_tiles_by_id(self, tile_ids, ds=None, **kwargs):

        if ds is not None:
            search = 'dataset_s:%s' % ds
        else:
            search = '*:*'

        additionalparams = {
            'fq': [
                "{!terms f=id}%s" % ','.join(tile_ids)
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        results = self.do_query_all(*(search, None, None, False, None), **additionalparams)

        assert len(results) == len(tile_ids), "Found %s results, expected exactly %s" % (len(results), len(tile_ids))
        return results

    def find_min_date_from_tiles(self, tile_ids, ds=None, **kwargs):

        if ds is not None:
            search = 'dataset_s:%s' % ds
        else:
            search = '*:*'

        kwargs['rows'] = 1
        kwargs['fl'] = 'tile_min_time_dt'
        kwargs['sort'] = ['tile_min_time_dt asc']
        additionalparams = {
            'fq': [
                "{!terms f=id}%s" % ','.join(tile_ids) if len(tile_ids) > 0 else ''
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        results, start, found = self.do_query(*(search, None, None, True, None), **additionalparams)

        return results[0]['tile_min_time_dt']

    def find_max_date_from_tiles(self, tile_ids, ds=None, **kwargs):

        if ds is not None:
            search = 'dataset_s:%s' % ds
        else:
            search = '*:*'

        kwargs['rows'] = 1
        kwargs['fl'] = 'tile_max_time_dt'
        kwargs['sort'] = ['tile_max_time_dt desc']
        additionalparams = {
            'fq': [
                "{!terms f=id}%s" % ','.join(tile_ids) if len(tile_ids) > 0 else ''
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        results, start, found = self.do_query(*(search, None, None, True, None), **additionalparams)

        return results[0]['tile_max_time_dt']

    def get_data_series_list(self):

        datasets = self.get_data_series_list_simple()

        for dataset in datasets:
            dataset['start'] = time.mktime(self.find_min_date_from_tiles([], ds=dataset['title']).timetuple()) * 1000
            dataset['end'] = time.mktime(self.find_max_date_from_tiles([], ds=dataset['title']).timetuple()) * 1000

        return datasets

    def get_data_series_list_simple(self):
        search = "*:*"
        params = {
            'rows': 0,
            "facet": "true",
            "facet.field": "dataset_s",
            "facet.mincount": "1"
        }

        response = self.do_query_raw(*(search, None, None, False, None), **params)
        l = []
        for g, v in response.facet_counts["facet_fields"]["dataset_s"].items():
            l.append({
                "shortName": g,
                "title": g,
                "tileCount": v
            })
        l = sorted(l, key=lambda entry: entry["title"])
        return l

    def find_tile_by_polygon_and_most_recent_day_of_year(self, bounding_polygon, ds, day_of_year):

        search = 'dataset_s:%s' % ds

        params = {
            'fq': [
                "{!field f=geo}Intersects(%s)" % bounding_polygon.wkt,
                "tile_count_i:[1 TO *]",
                "day_of_year_i:[* TO %s]" % day_of_year
            ],
            'rows': 1
        }

        results, start, found = self.do_query(
            *(search, None, None, True, ('day_of_year_i desc',)), **params)

        return [results[0]]

    def find_days_in_range_asc(self, min_lat, max_lat, min_lon, max_lon, ds, start_time, end_time, **kwargs):

        search = 'dataset_s:%s' % ds

        search_start_s = datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%SZ')
        search_end_s = datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%dT%H:%M:%SZ')

        additionalparams = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "{!frange l=0 u=0}ms(tile_min_time_dt,tile_max_time_dt)",
                "tile_count_i:[1 TO *]",
                "tile_min_time_dt:[%s TO %s] " % (search_start_s, search_end_s)
            ],
            'rows': 0,
            'facet': 'true',
            'facet_field': 'tile_min_time_dt',
            'facet_mincount': '1',
            'facet_limit': '-1'
        }

        self._merge_kwargs(additionalparams, **kwargs)

        response = self.do_query_raw(*(search, None, None, False, None), **additionalparams)

        daysinrangeasc = sorted(
            [(datetime.strptime(a_date, '%Y-%m-%dT%H:%M:%SZ') - datetime.utcfromtimestamp(0)).total_seconds() for a_date
             in response.facet_counts['facet_fields']['tile_min_time_dt'].keys()])

        return daysinrangeasc

    def find_all_tiles_in_box_sorttimeasc(self, min_lat, max_lat, min_lon, max_lon, ds, start_time=0,
                                          end_time=-1, **kwargs):

        search = 'dataset_s:%s' % ds

        additionalparams = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "tile_count_i:[1 TO *]"
            ]
        }

        if 0 < start_time <= end_time:
            search_start_s = datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%SZ')
            search_end_s = datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%dT%H:%M:%SZ')

            time_clause = "(" \
                          "tile_min_time_dt:[%s TO %s] " \
                          "OR tile_max_time_dt:[%s TO %s] " \
                          "OR (tile_min_time_dt:[* TO %s] AND tile_max_time_dt:[%s TO *])" \
                          ")" % (
                              search_start_s, search_end_s,
                              search_start_s, search_end_s,
                              search_start_s, search_end_s
                          )
            additionalparams['fq'].append(time_clause)

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(
            *(search, None, None, False, 'tile_min_time_dt asc, tile_max_time_dt asc'),
            **additionalparams)

    def find_all_tiles_in_polygon_sorttimeasc(self, bounding_polygon, ds, start_time=0, end_time=-1, **kwargs):

        search = 'dataset_s:%s' % ds

        additionalparams = {
            'fq': [
                "{!field f=geo}Intersects(%s)" % bounding_polygon.wkt,
                "tile_count_i:[1 TO *]"
            ]
        }

        if 0 < start_time <= end_time:
            search_start_s = datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%SZ')
            search_end_s = datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%dT%H:%M:%SZ')

            time_clause = "(" \
                          "tile_min_time_dt:[%s TO %s] " \
                          "OR tile_max_time_dt:[%s TO %s] " \
                          "OR (tile_min_time_dt:[* TO %s] AND tile_max_time_dt:[%s TO *])" \
                          ")" % (
                              search_start_s, search_end_s,
                              search_start_s, search_end_s,
                              search_start_s, search_end_s
                          )
            additionalparams['fq'].append(time_clause)

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(
            *(search, None, None, False, 'tile_min_time_dt asc, tile_max_time_dt asc'),
            **additionalparams)

    def find_all_tiles_in_polygon(self, bounding_polygon, ds, start_time=0, end_time=-1, **kwargs):

        search = 'dataset_s:%s' % ds

        additionalparams = {
            'fq': [
                "{!field f=geo}Intersects(%s)" % bounding_polygon.wkt,
                "tile_count_i:[1 TO *]"
            ]
        }

        if 0 < start_time <= end_time:
            search_start_s = datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%SZ')
            search_end_s = datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%dT%H:%M:%SZ')

            time_clause = "(" \
                          "tile_min_time_dt:[%s TO %s] " \
                          "OR tile_max_time_dt:[%s TO %s] " \
                          "OR (tile_min_time_dt:[* TO %s] AND tile_max_time_dt:[%s TO *])" \
                          ")" % (
                              search_start_s, search_end_s,
                              search_start_s, search_end_s,
                              search_start_s, search_end_s
                          )
            additionalparams['fq'].append(time_clause)

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(
            *(search, None, None, False, None),
            **additionalparams)

    def find_distinct_bounding_boxes_in_polygon(self, bounding_polygon, ds, start_time=0, end_time=-1, **kwargs):

        search = 'dataset_s:%s' % ds

        additionalparams = {
            'fq': [
                "{!field f=geo}Intersects(%s)" % bounding_polygon.wkt,
                "tile_count_i:[1 TO *]"
            ],
            'rows': 0,
            'facet': 'true',
            'facet.field': 'geo_s',
            'facet.limit': -1,
            'facet.mincount': 1
        }

        if 0 < start_time <= end_time:
            search_start_s = datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%SZ')
            search_end_s = datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%dT%H:%M:%SZ')

            time_clause = "(" \
                          "tile_min_time_dt:[%s TO %s] " \
                          "OR tile_max_time_dt:[%s TO %s] " \
                          "OR (tile_min_time_dt:[* TO %s] AND tile_max_time_dt:[%s TO *])" \
                          ")" % (
                              search_start_s, search_end_s,
                              search_start_s, search_end_s,
                              search_start_s, search_end_s
                          )
            additionalparams['fq'].append(time_clause)

        self._merge_kwargs(additionalparams, **kwargs)

        response = self.do_query_raw(*(search, None, None, False, None), **additionalparams)

        distinct_bounds = [wkt.loads(key).bounds for key in response.facet_counts["facet_fields"]["geo_s"].keys()]

        return distinct_bounds

    def find_tiles_by_exact_bounds(self, minx, miny, maxx, maxy, ds, start_time=0, end_time=-1, **kwargs):

        search = 'dataset_s:%s' % ds

        additionalparams = {
            'fq': [
                "tile_min_lon:\"%s\"" % minx,
                "tile_min_lat:\"%s\"" % miny,
                "tile_max_lon:\"%s\"" % maxx,
                "tile_max_lat:\"%s\"" % maxy,
                "tile_count_i:[1 TO *]"
            ]
        }

        if 0 < start_time <= end_time:
            search_start_s = datetime.utcfromtimestamp(start_time).strftime('%Y-%m-%dT%H:%M:%SZ')
            search_end_s = datetime.utcfromtimestamp(end_time).strftime('%Y-%m-%dT%H:%M:%SZ')

            time_clause = "(" \
                          "tile_min_time_dt:[%s TO %s] " \
                          "OR tile_max_time_dt:[%s TO %s] " \
                          "OR (tile_min_time_dt:[* TO %s] AND tile_max_time_dt:[%s TO *])" \
                          ")" % (
                              search_start_s, search_end_s,
                              search_start_s, search_end_s,
                              search_start_s, search_end_s
                          )
            additionalparams['fq'].append(time_clause)

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(
            *(search, None, None, False, None),
            **additionalparams)

    def find_all_tiles_in_box_at_time(self, min_lat, max_lat, min_lon, max_lon, ds, search_time, **kwargs):
        search = 'dataset_s:%s' % ds

        the_time = datetime.utcfromtimestamp(search_time).strftime('%Y-%m-%dT%H:%M:%SZ')
        time_clause = "(" \
                      "tile_min_time_dt:[* TO %s] " \
                      "AND tile_max_time_dt:[%s TO *] " \
                      ")" % (
                          the_time, the_time
                      )

        additionalparams = {
            'fq': [
                "geo:[%s,%s TO %s,%s]" % (min_lat, min_lon, max_lat, max_lon),
                "tile_count_i:[1 TO *]",
                time_clause
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(*(search, None, None, False, None), **additionalparams)

    def find_all_tiles_in_polygon_at_time(self, bounding_polygon, ds, search_time, **kwargs):
        search = 'dataset_s:%s' % ds

        the_time = datetime.utcfromtimestamp(search_time).strftime('%Y-%m-%dT%H:%M:%SZ')
        time_clause = "(" \
                      "tile_min_time_dt:[* TO %s] " \
                      "AND tile_max_time_dt:[%s TO *] " \
                      ")" % (
                          the_time, the_time
                      )

        additionalparams = {
            'fq': [
                "{!field f=geo}Intersects(%s)" % bounding_polygon.wkt,
                "tile_count_i:[1 TO *]",
                time_clause
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(*(search, None, None, False, None), **additionalparams)

    def find_all_tiles_within_box_at_time(self, min_lat, max_lat, min_lon, max_lon, ds, time, **kwargs):
        search = 'dataset_s:%s' % ds

        the_time = datetime.utcfromtimestamp(time).strftime('%Y-%m-%dT%H:%M:%SZ')
        time_clause = "(" \
                      "tile_min_time_dt:[* TO %s] " \
                      "AND tile_max_time_dt:[%s TO *] " \
                      ")" % (
                          the_time, the_time
                      )

        additionalparams = {
            'fq': [
                "geo:\"Within(ENVELOPE(%s,%s,%s,%s))\"" % (min_lon, max_lon, max_lat, min_lat),
                "tile_count_i:[1 TO *]",
                time_clause
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(*(search, "product(tile_avg_val_d, tile_count_i),*", None, False, None),
                                 **additionalparams)

    def find_all_boundary_tiles_at_time(self, min_lat, max_lat, min_lon, max_lon, ds, time, **kwargs):
        search = 'dataset_s:%s' % ds

        the_time = datetime.utcfromtimestamp(time).strftime('%Y-%m-%dT%H:%M:%SZ')
        time_clause = "(" \
                      "tile_min_time_dt:[* TO %s] " \
                      "AND tile_max_time_dt:[%s TO *] " \
                      ")" % (
                          the_time, the_time
                      )

        additionalparams = {
            'fq': [
                "geo:\"Intersects(MultiLineString((%s %s, %s %s),(%s %s, %s %s),(%s %s, %s %s),(%s %s, %s %s)))\"" % (
                    min_lon, max_lat, max_lon, max_lat, min_lon, max_lat, min_lon, min_lat, max_lon, max_lat, max_lon,
                    min_lat, min_lon, min_lat, max_lon, min_lat),
                "-geo:\"Within(ENVELOPE(%s,%s,%s,%s))\"" % (min_lon, max_lon, max_lat, min_lat),
                "tile_count_i:[1 TO *]",
                time_clause
            ]
        }

        self._merge_kwargs(additionalparams, **kwargs)

        return self.do_query_all(*(search, None, None, False, None), **additionalparams)

    def do_query(self, *args, **params):

        response = self.do_query_raw(*args, **params)

        return response.results, response.start, response.numFound

    def do_query_raw(self, *args, **params):

        # fl only works when passed as the second argument to solrcon.select
        if 'fl' in params.keys():
            fl = params['fl']
            del (params['fl'])
        else:
            fl = args[1]

        # sort only works when passed as the fourth argument to solrcon.select
        if 'sort' in params.keys():
            s = ','.join(params['sort'])
            del (params['sort'])
        else:
            s = args[4]

        # If dataset_s is specified as the search term,
        # add the _route_ parameter to limit the search to the correct shard
        if 'dataset_s:' in args[0]:
            ds = args[0].split(':')[-1]
            params['shard_keys'] = ds + '!'

        args = (args[0],) + (fl,) + (args[2:4]) + (s,)

        with SOLR_CON_LOCK:
            response = self.solrcon.select(*args, **params)

        return response

    def do_query_all(self, *args, **params):

        results = []

        response = self.do_query_raw(*args, **params)
        results.extend(response.results)

        limit = min(params.get('limit', float('inf')), response.numFound)

        while len(results) < limit:
            params['start'] = len(results)
            response = self.do_query_raw(*args, **params)
            results.extend(response.results)

        assert len(results) == limit

        return results

    def ping(self):
        solrAdminPing = 'http://%s/solr/%s/admin/ping' % (self.solrUrl, self.solrCore)
        try:
            r = requests.get(solrAdminPing, params={'wt': 'json'})
            results = json.loads(r.text)
            return results
        except:
            return None

    @staticmethod
    def _merge_kwargs(additionalparams, **kwargs):
        # Only Solr-specific kwargs are parsed
        # And the special 'limit'
        try:
            additionalparams['limit'] = kwargs['limit']
        except KeyError:
            pass

        try:
            additionalparams['_route_'] = kwargs['_route_']
        except KeyError:
            pass

        try:
            additionalparams['rows'] = kwargs['rows']
        except KeyError:
            pass

        try:
            additionalparams['start'] = kwargs['start']
        except KeyError:
            pass

        try:
            kwfq = kwargs['fq'] if isinstance(kwargs['fq'], list) else list(kwargs['fq'])
        except KeyError:
            kwfq = []

        try:
            additionalparams['fq'].extend(kwfq)
        except KeyError:
            additionalparams['fq'] = kwfq

        try:
            kwfl = kwargs['fl'] if isinstance(kwargs['fl'], list) else [kwargs['fl']]
        except KeyError:
            kwfl = []

        try:
            additionalparams['fl'].extend(kwfl)
        except KeyError:
            additionalparams['fl'] = kwfl

        try:
            s = kwargs['sort'] if isinstance(kwargs['sort'], list) else [kwargs['sort']]
        except KeyError:
            s = None

        try:
            additionalparams['sort'].extend(s)
        except KeyError:
            if s is not None:
                additionalparams['sort'] = s
