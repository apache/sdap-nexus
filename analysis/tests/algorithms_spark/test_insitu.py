from webservice.algorithms.doms import insitu
import json


"""
From Nga: 

https://doms.coaps.fsu.edu/ws/search/samos_cdms?startTime=2018-01-14T17%3A00%3A00Z&endTime=2018-01-14T17%3A53%3A00Z&bbox=-89%2C29%2C-88%2C30&itemsPerPage=1

https://doms.jpl.nasa.gov/insitu/1.0/query_data_doms?startIndex=0&itemsPerPage=1&provider=Florida%20State%20University%2C%20COAPS&project=SAMOS&startTime=2018-01-14T17%3A00%3A00Z&endTime=2018-01-14T17%3A53%3A00Z&bbox=-89%2C29%2C-88%2C30&minDepth=-1000&maxDepth=1000
"""


def test_query_insitu():
    # https://doms.jpl.nasa.gov/insitu/1.0/query_data_doms_custom_pagination?
    # itemsPerPage=20000
    # startTime=2017-05-01T01:54:45Z
    # endTime=2017-05-01T16:41:01Z
    # bbox=-100.0,20.0,-79.0,30.0
    # minDepth=-20.0
    # maxDepth=10.0
    # provider=Florida+State+University,+COAPS
    # project=SAMOS
    # platform=30
    response = insitu.query_insitu(
        dataset='SAMOS',
        variable=None,
        start_time='2017-05-01T01:54:45Z',
        end_time='2017-05-01T16:41:01Z',
        bbox='-100.0,20.0,-79.0,30.0',
        platform=30,
        depth_min=-20.0,
        depth_max=10.0,
        items_per_page=1000
    )

    print(f'{response=}')
    # assert False

def test_query_insitu_doms():
    # http://doms.coaps.fsu.edu/ws/search/samos?
    # startTime=2012-08-01T00:00:00Z&
    # endTime=2013-10-31T23:59:59Z&
    # bbox=-45,15,-30,30
    response = insitu.query_insitu(
        dataset='SAMOS',
        variable=None,
        start_time='2017-05-01T01:54:45Z',
        end_time='2017-05-01T16:41:01Z',
        bbox='-100.0,20.0,-79.0,30.0',
        platform=None,  # 30 seems to reduce the results to 0
        depth_min=-20.0,
        depth_max=10.0,
        items_per_page=1000
    )

    print(f'{response=}')


def test_insitu_result_serialization():
    with open('../data/edge_insitu_response.json') as fp:
        edge_result = json.loads(fp.read())

    with open('../data/parquet_insitu_response.json') as fp:
        parquet_result = json.loads(fp.read())

    parquet_result_model = insitu.InsituResult(**parquet_result)
    edge_result_model = insitu.InsituResult(**edge_result)

    assert parquet_result_model.results[0].platform == 30
    assert edge_result_model.results[0].platform is None

    assert parquet_result_model.results[0].metadata == 'WTEO_20180114v30001_0048-99999.0'
    assert edge_result_model.results[0].metadata == 'WTEO_20180114v30001_0048-99999.0'

    assert parquet_result_model.total_results == 4
    assert edge_result_model.total_results == 4



