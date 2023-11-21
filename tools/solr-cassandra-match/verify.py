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

import argparse
import uuid
import json
import asyncio

import logging
from datetime import datetime
from functools import partial
from time import sleep
from typing import List, Tuple, Union

import cassandra.concurrent
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy
from six.moves import input
from solrcloudpy import SolrConnection, SearchOptions
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)s] [%(levelname)s] [%(name)s::%(lineno)d] %(message)s"
    )

logger = logging.getLogger('delete-by-id')

logger.setLevel(logging.INFO)
logging.getLogger().handlers[0].setFormatter(
    logging.Formatter(
        fmt="%(asctime)s [%(threadName)s] [%(levelname)s] [%(name)s::%(lineno)d] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    ))

logging.getLogger('cassandra').setLevel(logging.CRITICAL)
logging.getLogger('solrcloudpy').setLevel(logging.CRITICAL)


BATCH_SIZE = 5000000


def compare(l1, l2):
    intersection = set(l1).intersection(set(l2))

    return len(intersection) == 0


def check_cassandra(args, ids):
    pass


def check_solr(args, ids):
    logger.info('Connecting to Solr...')

    solr_connection = SolrConnection(args.solr, timeout=60)
    solr_collection = solr_connection[args.collection]
    SOLR_UNIQUE_KEY = args.solrIdField

    logger.info('Fetching ids')

    se = SearchOptions()
    se.commonparams.q('*:*').fl(SOLR_UNIQUE_KEY).fl('id').rows(args.rows).sort('%s asc' % SOLR_UNIQUE_KEY)

    solr_ids = []

    next_cursor_mark = '*'

    while True:
        se.commonparams.remove_param('cursorMark')
        se.commonparams.add_params(cursorMark=next_cursor_mark)
        response = solr_collection.search(se)
        logger.debug('Executed Solr query for next page')

        try:
            result_next_cursor_mark = response.result.nextCursorMark
        except AttributeError:
            logger.debug('Query empty')
            return []

        if result_next_cursor_mark == next_cursor_mark:
            logger.debug('Reached end of Solr results')
            break
        else:
            next_cursor_mark = response.result.nextCursorMark

        solr_ids.extend([uuid.UUID(doc['id']) for doc in response.result.response.docs])

        logger.debug(f'Added {len(response.result.response.docs):,} docs ( -> {len(solr_ids):,}')

        if len(solr_ids) >= BATCH_SIZE:
            logger.debug('Running comparison')
            if not compare(solr_ids, ids):
                return False

            solr_ids.clear()

    logger.debug('Running final comparison')

    if not compare(solr_ids, ids):
        return False


def read_ids(args):
    logger.info(f'Reading ids from file {args.id_file}')

    with open(args.id_file) as f:
        ids = json.load(f)

        return [uuid.UUID(i) for i in ids]



def main(args):
    ids = read_ids(args)

    if args.target == 'solr':
        disjoint = check_solr(args, ids)
    else:
        disjoint = check_cassandra(args, ids)

    if disjoint:
        logger.info('THERE ARE SHARED TILE IDS')
    else:
        logger.info('All tile ids are absent from the target store')


def parse_args():
    parser = argparse.ArgumentParser(description='Verify that a list of tile ids is absent from either Solr or '
                                                 'Cassandra',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--target',
                        required=True,
                        choices=['solr', 'cassandra'],
                        help='Store to check',
                        dest='target')

    parser.add_argument('-i', '--id-list',
                        required=True,
                        dest='id_file',
                        help='Path to JSON file containing a list of tile UUIDs to delete')

    parser.add_argument('--solr',
                        help='The url of the SOLR server.',
                        default='localhost:8983',
                        metavar='127.0.0.1:8983')

    parser.add_argument('--collection',
                        help='The name of the SOLR collection.',
                        required=False,
                        default='nexustiles',
                        metavar='nexustiles')

    parser.add_argument('--solrIdField',
                        help='The name of the unique ID field for this collection.',
                        required=False,
                        default='id',
                        metavar='id')

    parser.add_argument('--cassandra',
                        help='The hostname(s) or IP(s) of the Cassandra server(s).',
                        default=['localhost'],
                        nargs='+',
                        metavar=('127.0.0.100', '127.0.0.101'))

    parser.add_argument('-k', '--cassandraKeyspace',
                        help='The Cassandra keyspace.',
                        default='nexustiles',
                        required=False,
                        metavar='nexustiles')

    parser.add_argument('-t', '--cassandraTable',
                        help='The name of the cassandra table.',
                        required=False,
                        default='sea_surface_temp')

    parser.add_argument('-p', '--cassandraPort',
                        help='The port used to connect to Cassandra.',
                        required=False,
                        default='9042')

    parser.add_argument('--cassandraUsername',
                        help='The username used to connect to Cassandra.',
                        default='cassandra',
                        required=False)

    parser.add_argument('--cassandraPassword',
                        help='The password used to connect to Cassandra.',
                        default='cassandra',
                        required=False)

    parser.add_argument('-pv', '--cassandraProtocolVersion',
                        help='The version of the Cassandra protocol the driver should use.',
                        required=False,
                        choices=['1', '2', '3', '4', '5'],
                        default='3')

    parser.add_argument('--solr-rows',
                        help='Size of Solr query pages to check',
                        required=False,
                        dest='rows',
                        default=100000,
                        type=int)

    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Enable verbose output')

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()

    if args.verbose:
        logger.setLevel(logging.DEBUG)

    main(args)


