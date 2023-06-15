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

CASSANDRA_BATCH_SIZE = 8192
SOLR_BATCH_SIZE = 256


def delete_from_solr(args, ids):
    pass


def delete_from_cassandra(args, ids):
    logger.info('Trying to connect to Cassandra...')

    dc_policy = RoundRobinPolicy()
    token_policy = TokenAwarePolicy(dc_policy)

    if args.cassandraUsername and args.cassandraPassword:
        auth_provider = PlainTextAuthProvider(username=args.cassandraUsername, password=args.cassandraPassword)
    else:
        auth_provider = None

    cassandra_cluster = Cluster(contact_points=args.cassandra, port=args.cassandraPort,
                                protocol_version=int(args.cassandraProtocolVersion),
                                execution_profiles={
                                    EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=token_policy)
                                },
                                auth_provider=auth_provider)
    cassandra_session = cassandra_cluster.connect(keyspace=args.cassandraKeyspace)

    logger.info('Successfully connected to Cassandra')

    cassandra_table = args.cassandraTable

    batches = [ids[i:i + CASSANDRA_BATCH_SIZE] for i in range(0, len(ids), CASSANDRA_BATCH_SIZE)]

    logger.info(f'Prepared {len(batches):,} batches of tile ids to delete')

    start = datetime.now()

    n_tiles = len(ids)
    deleting = 0

    prepared_query = cassandra_session.prepare('DELETE FROM %s WHERE tile_id=?' % cassandra_table)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=10, max=60))
    def delete_batch(batch):
        for tile_id in batch:
            futures.append(cassandra_session.execute_async(prepared_query, (tile_id,)))

        for f in futures:
            try:
                f.result()
            except:
                logger.warning('Batch delete failed; maybe retrying')
                raise

    for batch in batches:
        futures = []

        deleting += len(batch)

        logger.info(
            f'Deleting batch of {len(batch)} tiles from Cassandra | '
            f'({deleting}/{n_tiles}) [{deleting / n_tiles * 100:7.3f}%]')

        try:
            delete_batch(batch)
        except:
            logger.critical('Failed to delete batch after multiple retries, exiting')
            exit(1)

    logger.info(f'Deleted {len(ids):,} tiles from Cassandra in {str(datetime.now() - start)} seconds')


def read_ids(args):
    logger.info(f'Reading ids from file {args.id_file}')

    with open(args.id_file) as f:
        ids = json.load(f)

        return [uuid.UUID(i) for i in ids]


def main(args):
    ids = read_ids(args)

    logger.info(f'Successfully read {len(ids):,} tile ids to delete')

    if args.target == 'solr':
        delete_from_solr(args, ids)
    else:
        delete_from_cassandra(args, ids)


def parse_args():
    parser = argparse.ArgumentParser(description='Delete a list of tile ids from either Solr or Cassandra',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--target',
                        required=True,
                        choices=['solr', 'cassandra'],
                        help='Store to delete data from',
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

    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Enable verbose output')

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    main(args)


