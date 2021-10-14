# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import logging
import threading
import time
import re
from datetime import datetime
from pytz import timezone, UTC

import requests
import pysolr
from shapely import wkt
from elasticsearch import Elasticsearch

ELASTICSEARCH_CON_LOCK = threading.Lock()
thread_local = threading.local()

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))
ELASTICSEARCH_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
ISO_8601 = '%Y-%m-%dT%H:%M:%S%z'


class ElasticsearchProxy(object):
    def __init__(self, config):
        self.elasticsearchHosts = config.get("elasticsearch", "host").split(',')
        self.elasticsearchIndex = config.get("elasticsearch", "index")
        self.elasticsearchUsername = config.get("elasticsearch", "username")
        self.elasticsearchPassword = config.get("elasticsearch", "password")
        self.logger = logging.getLogger(__name__)

        with ELASTICSEARCH_CON_LOCK:
            elasticsearchcon = getattr(thread_local, 'elasticsearchcon', None)
            if elasticsearchcon is None:
                elasticsearchcon = Elasticsearch(hosts=self.elasticsearchHosts, http_auth=(self.elasticsearchUsername, self.elasticsearchPassword))
                thread_local.elasticsearchcon = elasticsearchcon

            self.elasticsearchcon = elasticsearchcon

    def find_tile_by_id(self, tile_id):

        params = {
            "size": 1,
            "query": {
                "term": {
                    "id": {
                        "value": tile_id
                    }
                }
            }
        }

        results, _, hits = self.do_query(*(None, None, None, True, None), **params)
        assert hits == 1, f"Found {hits} results, expected exactly 1"
        return [results[0]["_source"]]

    def find_tiles_by_id(self, tile_ids, ds=None, **kwargs):

        params = {
            "query": {
                "bool": {
                    "filter": [],
                    "should": [],
                    "minimum_should_match": 1
                }
            }
        }

        for tile_id in tile_ids:
            params['query']['bool']['should'].append({"term": {"id": {"value": tile_id}}})
            
        if ds is not None:
            params['query']['bool']['filter'].append({"term": {"dataset_s": {"value": ds}}})

        self._merge_kwargs(params, **kwargs)

        results = self.do_query_all(*(None, None, None, False, None), **params)
        assert len(results) == len(tile_ids), "Found %s results, expected exactly %s" % (len(results), len(tile_ids))
        return results

    def find_min_date_from_tiles(self, tile_ids, ds=None, **kwargs):
        params = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [],
                    "should": []
                }
            },
            "aggs": {
                "min_date_agg": {
                    "min": {
                        "field": "tile_min_time_dt"
                    }
                }
            }            
        }
        
        for tile_id in tile_ids:
            params['query']['bool']['should'].append({"term": {"id": {"value": tile_id}}})
        if ds is not None:
            params['query']['bool']['filter'].append({"term": {"dataset_s": {"value": ds}}})

        aggregations = self.do_aggregation(*(None, None, None, True, None), **params)
        return self.convert_iso_to_datetime(aggregations['min_date_agg']["value_as_string"])

    def find_max_date_from_tiles(self, tile_ids, ds=None, **kwargs):

        params = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [],
                    "should": []
                }
            },
            "aggs": {
                "max_date_agg": {
                    "max": {
                        "field": "tile_max_time_dt"
                    }
                }
            }            
        }
        
        for tile_id in tile_ids:
            params['query']['bool']['should'].append({"term": {"id": {"value": tile_id}}})
        if ds is not None:
            params['query']['bool']['filter'].append({"term": {"dataset_s": {"value": ds}}})        

        aggregations = self.do_aggregation(*(None, None, None, True, None), **params)
        return self.convert_iso_to_datetime(aggregations['max_date_agg']["value_as_string"])


    def find_min_max_date_from_granule(self, ds, granule_name, **kwargs):
        
        params = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "dataset_s": {
                                    "value": ds
                                }
                            }
                        },
                        {
                            "term": {
                                "granule_s": {
                                    "value": granule_name
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "min_date_agg": {
                    "max": {
                        "field": "tile_min_time_dt"
                    }
                },
                "max_date_agg": {
                    "max": {
                        "field": "tile_max_time_dt"
                    }
                }
            }
        }

        self._merge_kwargs(params, **kwargs)
        
        aggregations = self.do_aggregation(*(None, None, None, False, None), **params)
        start_time = self.convert_iso_to_datetime(aggregations['min_date_agg']["value_as_string"])
        end_time = self.convert_iso_to_datetime(aggregations['max_date_agg']["value_as_string"])

        return start_time, end_time

    def get_data_series_list(self):

        datasets = self.get_data_series_list_simple()

        for dataset in datasets:
            min_date = self.find_min_date_from_tiles([], ds=dataset['title'])
            max_date = self.find_max_date_from_tiles([], ds=dataset['title'])
            dataset['start'] = (min_date - EPOCH).total_seconds()
            dataset['end'] = (max_date - EPOCH).total_seconds()
            dataset['iso_start'] = min_date.strftime(ISO_8601)
            dataset['iso_end'] = max_date.strftime(ISO_8601)

        return datasets

    def get_data_series_list_simple(self):
        
        params = {
            'size': 0,
            "aggs": {
                "dataset_list_agg": {
                    "composite": {
                        "size":100,
                        "sources": [
                            {
                                "dataset_s": {
                                    "terms": {
                                        "field": "dataset_s"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }

        aggregations = self.do_aggregation_all(params, 'dataset_list_agg')
        l = []

        for dataset in aggregations:
            l.append({
                "shortName": dataset['key']['dataset_s'],
                "title": dataset['key']['dataset_s'],
                "tileCount": dataset["doc_count"]
            })

        l = sorted(l, key=lambda entry: entry["title"])
        return l

    def get_data_series_stats(self, ds):

        params = {
            "size": 0,
            "query": {
                "term":{
                    "dataset_s": {
                        "value": ds
                    }
                }     
            },
            "aggs": {
                "available_dates": {
                    "composite": {
                        "size": 100,
                        "sources": [
                            {"terms_tile_max_time_dt": {"terms": {"field": "tile_max_time_dt"}}}
                        ]
                    }
                }
            }
        }

        aggregations = self.do_aggregation_all(params, 'available_dates')
        stats = {}
        stats['available_dates'] = []

        for dt in aggregations:
            stats['available_dates'].append(dt['key']['terms_tile_max_time_dt'] / 1000)

        stats['available_dates'] = sorted(stats['available_dates'])

        params = {
            "size": 0,
            "query": {
                "term":{
                    "dataset_s": {
                        "value": ds
                    }
                }
            }, 
            "aggs": {
                "min_tile_min_val_d": {
                    "min": {
                        "field": "tile_min_val_d"
                    }
                },
                "min_tile_max_time_dt": {
                    "min": {
                        "field": "tile_max_time_dt"
                    }
                },
                "max_tile_max_time_dt": {
                    "max": {
                        "field": "tile_max_time_dt"
                    }
                },
                "max_tile_max_val_d": {
                    "max": {
                        "field": "tile_max_val_d"
                    }
                }
            }
        }

        aggregations = self.do_aggregation(*(None, None, None, False, None), **params)
        stats["start"] = int(aggregations["min_tile_max_time_dt"]["value"]) / 1000
        stats["end"] = int(aggregations["max_tile_max_time_dt"]["value"]) / 1000
        stats["minValue"] = aggregations["min_tile_min_val_d"]["value"]
        stats["maxValue"] = aggregations["max_tile_max_val_d"]["value"]

        return stats

    # day_of_year_i added (SDAP-347)
    def find_tile_by_polygon_and_most_recent_day_of_year(self, bounding_polygon, ds, day_of_year):

        max_lat = bounding_polygon.bounds[3]
        min_lon = bounding_polygon.bounds[0]
        min_lat = bounding_polygon.bounds[1]
        max_lon = bounding_polygon.bounds[2]
        
        params = {
            "size": "1",
            "query": {
                "bool": {
                    "filter": [
                        {   
                            "term": {
                                "dataset_s": {
                                    "value": ds
                                }
                            }
                        },
                        {
                            "geo_shape": {
                                "geo": {
                                    "shape": {
                                        "type": "envelope",
                                        "coordinates": [[min_lon, max_lat], [max_lon, min_lat]]
                                    },
                                    "relation": "intersects"
                                }
                            }
                        },
                        { 
                            "range": {
                                "tile_count_i": {
                                    "gte": 1
                                }
                            }   
                        },
                        { 
                            "range": {
                                "day_of_year_i": {
                                    "lte": day_of_year
                                }
                            } 
                        }
                    ]
                }
            }
        }
        result, _, _ = self.do_query(*(None, None, None, True, 'day_of_year_i desc'), **params)
        
        return [result[0]]

    def find_days_in_range_asc(self, min_lat, max_lat, min_lon, max_lon, ds, start_time, end_time, **kwargs):

        search_start_s = datetime.utcfromtimestamp(start_time).strftime(ELASTICSEARCH_FORMAT)
        search_end_s = datetime.utcfromtimestamp(end_time).strftime(ELASTICSEARCH_FORMAT)

        params = {
            "size": "0",
            "_source": "tile_min_time_dt",
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "dataset_s": {
                                    "value": ds
                                }
                            }
                        },
                        {
                            "range": {
                                "tile_min_time_dt": {
                                    "gte": search_start_s,
                                    "lte": search_end_s
                                }
                            }
                        },
                        {
                            "geo_shape": {
                                "geo": {
                                    "shape": {
                                        "type": "envelope",
                                        "coordinates": [[min_lon, max_lat],[max_lon, min_lat]]
                                    },
                                    "relation": "intersects"
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "days_range_agg": {
                    "composite": {
                        "size":100,
                        "sources": [
                            {
                                "tile_min_time_dt": {
                                    "terms": {
                                        "field": "tile_min_time_dt"
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }

        aggregations = self.do_aggregation_all(params, 'days_range_agg')
        results = [res['key']['tile_min_time_dt'] for res in aggregations]
        daysinrangeasc = sorted([(res / 1000) for res in results])
        return daysinrangeasc

    def find_all_tiles_in_box_sorttimeasc(self, min_lat, max_lat, min_lon, max_lon, ds, start_time=0,
                                          end_time=-1, **kwargs):

        params = {
            "size": 1000,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "dataset_s": {
                                    "value": ds
                                }
                            }
                        },
                        {
                            "geo_shape": {
                                "geo": {
                                    "shape": {
                                        "type": "envelope",
                                        "coordinates": [[min_lon, max_lat],[max_lon, min_lat]]
                                    },
                                    "relation": "intersects"
                                }
                            }
                        },
                        {
                            "range": {
                                "tile_count_i": {
                                    "gte": 1
                                 }
                             }
                         }
                     ]
                 }
             }
         }
                            

        if 0 < start_time <= end_time:
            params["query"]["bool"]["should"] = self.get_formatted_time_clause(start_time, end_time)
            params["query"]["bool"]["minimum_should_match"] = 1

        self._merge_kwargs(params, **kwargs)

        return self.do_query_all(*(None, None, None, False, 'tile_min_time_dt asc,tile_max_time_dt asc'), **params)

    def find_all_tiles_in_polygon_sorttimeasc(self, bounding_polygon, ds, start_time=0, end_time=-1, **kwargs):

        nums = re.findall(r'\d+(?:\.\d*)?', bounding_polygon.wkt.rpartition(',')[0])
        polygon_coordinates = list(zip(*[iter(nums)] * 2))

        max_lat = bounding_polygon.bounds[3]
        min_lon = bounding_polygon.bounds[0]
        min_lat = bounding_polygon.bounds[1]
        max_lon = bounding_polygon.bounds[2]

        params = {
            "query": {
                "bool": {
                    "filter": [
                        { 
                            "term": {
                                "dataset_s": {
                                    "value": ds
                                }
                            } 
                        },
                        {
                            "geo_shape": {
                                "geo": {
                                    "shape": {
                                        "type": "envelope",
                                        "coordinates": [[min_lon, max_lat], [max_lon, min_lat]]
                                    },
                                    "relation": "intersects"
                                }
                            }
                        }
                    ]
                }
            }
        }

        try:
            if 'fl' in list(kwargs.keys()):
                params["_source"] = kwargs["fl"].split(',')
        except KeyError:
            pass

        if 0 < start_time <= end_time:
            params["query"]["bool"]["should"] = self.get_formatted_time_clause(start_time, end_time)
            params["query"]["bool"]["minimum_should_match"] = 1

        return self.do_query_all(*(None, None, None, False, 'tile_min_time_dt asc,tile_max_time_dt asc'), **params)

    def find_all_tiles_in_polygon(self, bounding_polygon, ds, start_time=0, end_time=-1, **kwargs):

        nums = re.findall(r'\d+(?:\.\d*)?', bounding_polygon.wkt.rpartition(',')[0])
        polygon_coordinates = list(zip(*[iter(nums)] * 2))

        max_lat = bounding_polygon.bounds[3]
        min_lon = bounding_polygon.bounds[0]
        min_lat = bounding_polygon.bounds[1]
        max_lon = bounding_polygon.bounds[2]

        params = {
            "size": 1000,
            "query": {
                "bool": {
                    "filter": [
                        { 
                            "term": {
                                "dataset_s": {
                                    "value": ds
                                }
                            } 
                        },
                        {
                            "geo_shape": {
                                "geo": {
                                    "shape": {
                                        "type": "envelope",
                                        "coordinates": [[min_lon, max_lat], [max_lon, min_lat]]
                                    },
                                    "relation": "intersects"
                                }
                            }
                        },
                        {
                            "range": {
                                "tile_count_i": {
                                    "gte": 1
                                }
                            }
                        }
                    ]
                }
            }
        }

        try:
            if 'fl' in list(kwargs.keys()):
                params["_source"] = kwargs["fl"].split(',')
        except KeyError:
            pass

        if 0 < start_time <= end_time:
            params["query"]["bool"]["should"] = self.get_formatted_time_clause(start_time, end_time)
            params["query"]["bool"]["minimum_should_match"] = 1

        self._merge_kwargs(params, **kwargs)

        return self.do_query_all(*(None, None, None, False, None), **params)

    def find_distinct_bounding_boxes_in_polygon(self, bounding_polygon, ds, start_time=0, end_time=-1, **kwargs):
        
        tile_max_lat = bounding_polygon.bounds[3]
        tile_min_lon = bounding_polygon.bounds[0]
        tile_min_lat = bounding_polygon.bounds[1]
        tile_max_lon = bounding_polygon.bounds[2]

        params = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "dataset_s": {
                                    "value": ds
                                }
                            }
                        },
                        {
                            "geo_shape": {
                                "geo": {
                                    "shape": {
                                        "type": "envelope",
                                        "coordinates": [[tile_min_lon, tile_max_lat], [tile_max_lon, tile_min_lat]]
                                    },
                                    "relation": "intersects"
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "distinct_bounding_boxes": {
                    "composite": {
                        "size": 100,
                        "sources": [
                            {
                                "bounding_box": {
                                    "terms": {
                                        "script": {
                                            "source": "String.valueOf(doc['tile_min_lon'].value) + ', ' + String.valueOf(doc['tile_max_lon'].value) + ', ' + String.valueOf(doc['tile_min_lat'].value) + ', ' + String.valueOf(doc['tile_max_lat'].value)",
                                            "lang": "painless"
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        }
                            
        if 0 < start_time <= end_time:
            params["query"]["bool"]["should"] = self.get_formatted_time_clause(start_time, end_time)
            params["query"]["bool"]["minimum_should_match"] = 1

        self._merge_kwargs(params, **kwargs)
        aggregations = self.do_aggregation_all(params, 'distinct_bounding_boxes')
        distinct_bounds = []
        for agg in aggregations:   
            coords = agg['key']['bounding_box'].split(',')
            min_lon = round(float(coords[0]), 2)
            max_lon = round(float(coords[1]), 2)
            min_lat = round(float(coords[2]), 2)
            max_lat = round(float(coords[3]), 2)
            polygon = 'POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))' % (min_lon, max_lat, min_lon, min_lat, max_lon, min_lat, max_lon, max_lat, min_lon, max_lat)
            distinct_bounds.append(wkt.loads(polygon).bounds)
        
        return distinct_bounds
    
    def find_tiles_by_exact_bounds(self, minx, miny, maxx, maxy, ds, start_time=0, end_time=-1, **kwargs):
            
        params = {
            "query": {
                "bool": {
                    "filter": [
                    {
                        "term": {
                            "dataset_s": {
                                "value": ds
                            }
                        }
                    },
                    {
                        "term": {
                            "tile_min_lon": {
                                "value": minx
                            }
                        }
                    },
                    {
                        "term": {
                            "tile_min_lat": {
                                "value": miny
                            }
                        }
                    },
                    {
                        "term": {
                            "tile_max_lon": {
                                "value": maxx
                            }
                        }
                    },
                    {
                        "term": {
                            "tile_max_lat": {
                                "value": maxy
                            }
                        }
                    }
                ]
            }
        }}    
        
        if 0 < start_time <= end_time:
            params["query"]["bool"]["should"] = self.get_formatted_time_clause(start_time, end_time)
            params["query"]["bool"]["minimum_should_match"] = 1

        self._merge_kwargs(params, **kwargs)

        return self.do_query_all(*(None, None, None, False, None), **params)

    def find_all_tiles_in_box_at_time(self, min_lat, max_lat, min_lon, max_lon, ds, search_time, **kwargs):
        
        the_time = datetime.utcfromtimestamp(search_time).strftime(ELASTICSEARCH_FORMAT)

        params = {
            "size": 1000,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "dataset_s": {
                                    "value": ds
                                }
                            }
                        },
                        {
                            "geo_shape": {
                                "geo": {
                                    "shape": {
                                        "type": "envelope",
                                        "coordinates": [[min_lon, max_lat],[max_lon, min_lat]]
                                    },
                                    "relation": "intersects"
                                }
                            }
                        },
                        { 
                            "range": {
                                "tile_min_time_dt": {
                                    "lte": the_time
                                }
                            }   
                        },
                        { 
                            "range": {
                                "tile_max_time_dt": {
                                    "gte": the_time
                                }
                            } 
                        }
                    ]
                }
            }
        }

        self._merge_kwargs(params, **kwargs)

        return self.do_query_all(*(None, None, None, False, None), **params)

    def find_all_tiles_in_polygon_at_time(self, bounding_polygon, ds, search_time, **kwargs):

        the_time = datetime.utcfromtimestamp(search_time).strftime(ELASTICSEARCH_FORMAT)

        max_lat = bounding_polygon.bounds[3]
        min_lon = bounding_polygon.bounds[0]
        min_lat = bounding_polygon.bounds[1]
        max_lon = bounding_polygon.bounds[2]

        params = {
            "size": 1000,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "dataset_s": {
                                    "value": ds
                                }
                            }
                        },
                        {
                            "geo_shape": {
                                "geo": {
                                    "shape": {
                                        "type": "envelope",
                                        "coordinates": [[min_lon, max_lat],[max_lon, min_lat]]
                                    },
                                    "relation": "intersects"
                                }
                            }
                        },
                        { "range": {
                            "tile_min_time_dt": {
                                "lte": the_time
                            }
                        } },
                        { "range": {
                            "tile_max_time_dt": {
                                "gte": the_time
                            }
                        } }
                    ]
                }
            }
        }
        
        self._merge_kwargs(params, **kwargs)

        return self.do_query_all(*(None, None, None, False, None), **params)


    def find_all_tiles_within_box_at_time(self, min_lat, max_lat, min_lon, max_lon, ds, time, **kwargs):

        the_time = datetime.utcfromtimestamp(time).strftime(ELASTICSEARCH_FORMAT)
        
        params = {
            "size": 1000,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "dataset_s": {
                                    "value": ds
                                }
                            }
                        },
                        {
                            "geo_shape": {
                                "geo": {
                                    "shape": {
                                        "type": "envelope",
                                        "coordinates": [[min_lon, max_lat],[max_lon, min_lat]]
                                    },
                                    "relation": "within"
                                }
                            }
                        },
                        {
                            "range": {
                                "tile_count_i": {
                                    "gte": 1
                                }
                            }
                        },
                        { 
                            "range": {
                                "tile_min_time_dt": {
                                    "lte": the_time
                                }
                            } 
                        },
                        { 
                            "range": {
                                "tile_max_time_dt": {
                                    "gte": the_time
                                }
                            }
                        }
                    ]
                }
            }
        }


        self._merge_kwargs(params, **kwargs)

        return self.do_query_all(*(None, "product(tile_avg_val_d, tile_count_i),*", None, False, None), **params)

    def find_all_boundary_tiles_at_time(self, min_lat, max_lat, min_lon, max_lon, ds, time, **kwargs):
        
        the_time = datetime.utcfromtimestamp(time).strftime(ELASTICSEARCH_FORMAT)
        
        params = {
            "size": 1000,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "dataset_s": {
                                    "value": ds
                                }
                            }
                        },
                        {
                            "geo_shape": {
                                "geo": {
                                    "shape": {
                                        "type": "multilinestring",
                                        "coordinates": [[[min_lon, max_lat], [max_lon, max_lat], [min_lon, max_lat], [min_lon, min_lat], [max_lon, max_lat], [max_lon, min_lat], [min_lon, min_lat], [max_lon, min_lat]]]
                                    },
                                    "relation": "intersects"
                                }
                            }
                        },
                        {
                            "range": {
                                "tile_count_i": {
                                    "gte": 1
                                }
                            }
                        },
                        { 
                            "range": {
                                "tile_min_time_dt": {
                                    "lte": the_time
                                }
                            } 
                        },
                        { 
                            "range": {
                                "tile_max_time_dt": {
                                    "gte": the_time
                                }
                            }
                        }
                    ],
                    "must_not" : {
                        "geo_shape": {
                            "geo": {
                                "shape": {
                                    "type": "envelope",
                                    "coordinates": [[min_lon, max_lat], [max_lon, min_lat]]
                                },
                                "relation": "within"
                            }
                        }
                    }
                }
            }
        }

        self._merge_kwargs(params, **kwargs)

        return self.do_query_all(*(None, None, None, False, None), **params)

    def find_all_tiles_by_metadata(self, metadata, ds, start_time=0, end_time=-1, **kwargs):
        """
        Get a list of tile metadata that matches the specified metadata, start_time, end_time.
        :param metadata: List of metadata values to search for tiles e.g ["river_id_i:1", "granule_s:granule_name"]
        :param ds: The dataset name to search
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :return: A list of tile metadata
        """

        params = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "dataset_s": {"value": ds}
                            }
                        }
                    ]
                }
            }
        }

        if len(metadata) > 0:
            for key_value in metadata:
                key = key_value.split(':')[0]
                value = key_value.split(':')[1]
                params['query']['bool']['must'].append({"match": {key: value}})

        if 0 < start_time <= end_time:
            params['query']['bool']['should'] = self.get_formatted_time_clause(start_time, end_time)
            params["query"]["bool"]["minimum_should_match"] = 1

        self._merge_kwargs(params, **kwargs)
        return self.do_query_all(*(None, None, None, False, None), **params)

    def get_formatted_time_clause(self, start_time, end_time):
        search_start_s = datetime.utcfromtimestamp(start_time).strftime(ELASTICSEARCH_FORMAT)
        search_end_s = datetime.utcfromtimestamp(end_time).strftime(ELASTICSEARCH_FORMAT)
 
        time_clause = [ 
            { 
                "range": {
                    "tile_min_time_dt": {
                        "lte": search_end_s,
                        "gte": search_start_s    
                    }
                }
            },
            { 
                "range": {
                    "tile_max_time_dt": {
                        "lte": search_end_s,
                        "gte": search_start_s
                    }
                }
            },
            { 
                "bool": { 
                    "must": [
                        { 
                            "range": {
                                "tile_min_time_dt": {
                                    "gte": search_start_s
                                }
                            } 
                        },
                        {
                            "range": {
                                "tile_max_time_dt": {
                                    "lte": search_end_s
                                }
                            } 
                        }
                    ] 
                } 
            }
        ]

        return time_clause

    def get_tile_count(self, ds, bounding_polygon=None, start_time=0, end_time=-1, metadata=None, **kwargs):
        """
        Return number of tiles that match search criteria.
        :param ds: The dataset name to search
        :param bounding_polygon: The polygon to search for tiles
        :param start_time: The start time to search for tiles
        :param end_time: The end time to search for tiles
        :param metadata: List of metadata values to search for tiles e.g ["river_id_i:1", "granule_s:granule_name"]
        :return: number of tiles that match search criteria
        """
        
        params = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "dataset_s": {
                                    "value": ds
                                }
                            }
                        },
                        {
                            "range": {
                                "tile_count_i": {
                                    "gte": 1
                                }
                            }
                        }
                    ]
                }
            }
        }

        if bounding_polygon:
            min_lon, min_lat, max_lon, max_lat = bounding_polygon.bounds
            geo_clause = {
                "geo_shape": {
                    "geo": {
                        "shape": {
                            "type": "envelope",
                            "coordinates": [[min_lon, max_lat], [max_lon, min_lat]]
                        }
                    }
                }
            }
                
            params['query']['bool']['filter'].append(geo_clause)

        if 0 < start_time <= end_time:
            params['query']['bool']['should'] = self.get_formatted_time_clause(start_time, end_time)
            params["query"]["bool"]["minimum_should_match"] = 1

        if len(metadata) > 0:
            for key_value in metadata:
                key = key_value.split(':')[0]
                value = key_value.split(':')[1]
                params['query']['bool']['filter'].append({"term": {key: {"value": value}}})

        self._merge_kwargs(params, **kwargs)
        _, _, found = self.do_query(*(None, None, None, True, None), **params)

        return found
    
    def do_aggregation(self, *args, **params):
        # Gets raw aggregations

        response = self.do_query_raw(*args, **params)
        aggregations = response.get('aggregations', None)
        return aggregations

    def do_aggregation_all(self, params, agg_name):
        # Used for pagination when results can exceed ES max size (use of after_key)

        with ELASTICSEARCH_CON_LOCK:
            response = self.elasticsearchcon.search(index=self.elasticsearchIndex, body=params)
        all_buckets = []
        
        try:
            aggregations = response.get('aggregations', None)
            current_buckets = aggregations.get(agg_name, None)
            buckets = current_buckets.get('buckets', None)
            all_buckets += buckets
            after_bucket = current_buckets.get('after_key', None)    
            
            while after_bucket is not None:
                for agg in params['aggs']:
                    params['aggs'][agg]['composite']['after'] = {}
                    for source in params['aggs'][agg]['composite']['sources']:
                        key_name = next(iter(source))
                        params['aggs'][agg]['composite']['after'][key_name] = after_bucket[key_name]
                with ELASTICSEARCH_CON_LOCK:
                    response = self.elasticsearchcon.search(index=self.elasticsearchIndex, body=params)
                
                aggregations = response.get('aggregations', None)
                current_buckets = aggregations.get(agg_name, None)
                buckets = current_buckets.get('buckets', None)
                all_buckets += buckets
                after_bucket = current_buckets.get('after_key', None)
                
        except AttributeError as e:
            self.logger.error('Error when accessing aggregation buckets - ' + str(e))

        return all_buckets

    def do_query(self, *args, **params):
        response = self.do_query_raw(*args, **params)
        return response['hits']['hits'], None, response['hits']['total']['value']

    def do_query_raw(self, *args, **params):

        if args[4]:

            sort_fields = args[4].split(",")

            if 'sort' not in list(params.keys()):
                params["sort"] = []

            for field in sort_fields:
                field_order = field.split(' ')
                sort_instruction = {field_order[0]: field_order[1]}
                if sort_instruction not in params['sort']:
                    params["sort"].append(sort_instruction)
        with ELASTICSEARCH_CON_LOCK:
            response = self.elasticsearchcon.search(index=self.elasticsearchIndex, body=params)
        
        return response

    def do_query_all(self, *args, **params):
        # Used to paginate with search_after. 
        # The method calling this might already have a sort clause, 
        # so we merge both sort clauses inside do_query_raw
        
        results = []

        search = None
        
        # Add track option to not be blocked at 10000 hits per worker
        if 'track_total_hits' not in params.keys():
            params['track_total_hits'] = True

        # Add sort instruction order to paginate the results :
        params["sort"] = [
            { "tile_min_time_dt": "asc"},
            { "_id": "asc" }
        ]

        response = self.do_query_raw(*args, **params)
        results.extend([r["_source"] for r in response["hits"]["hits"]])

        total_hits = response["hits"]["total"]["value"]

        try:
            search_after = []
            for sort_param in response["hits"]["hits"][-1]["sort"]:
                search_after.append(str(sort_param))
        except (KeyError, IndexError):
            search_after = []

        try:
            while len(results) < total_hits:
                params["search_after"] = search_after
                response = self.do_query_raw(*args, **params)
                results.extend([r["_source"] for r in response["hits"]["hits"]])
                
                search_after = []
                for sort_param in response["hits"]["hits"][-1]["sort"]:
                    search_after.append(str(sort_param))
        
        except (KeyError, IndexError):
            pass

        return results

    def convert_iso_to_datetime(self, date):
        return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)

    def convert_iso_to_timestamp(self, date):
        return (self.convert_iso_to_datetime(date) - EPOCH).total_seconds()

    @staticmethod
    def _merge_kwargs(params, **kwargs):
        # Only Solr-specific kwargs are parsed
        # And the special 'limit'
        try:
            params['limit'] = kwargs['limit']
        except KeyError:
            pass

        try:
            params['_route_'] = kwargs['_route_']
        except KeyError:
            pass

        try:
            params['size'] = kwargs['size']
        except KeyError:
            pass

        try:
            params['start'] = kwargs['start']
        except KeyError:
            pass

        try:
            s = kwargs['sort'] if isinstance(kwargs['sort'], list) else [kwargs['sort']]
        except KeyError:
            s = None

        try:
            params['sort'].extend(s)
        except KeyError:
            if s is not None:
                params['sort'] = s
