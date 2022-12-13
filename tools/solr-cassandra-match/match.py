import argparse
import json
import logging
from time import sleep
import uuid
from typing import List, Tuple

import cassandra.concurrent
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy
from six.moves import input
from solrcloudpy import SolrConnection, SearchOptions

solr_connection = None
solr_collection = None
SOLR_UNIQUE_KEY = None

cassandra_cluster = None
cassandra_session = None
cassandra_table = None

logger = None

PROCEED_THRESHOLD = 300000


class Encoder(json.JSONEncoder):
    def __init__(self, **args):
        json.JSONEncoder.__init__(self, **args)

    def default(self, o):
        if isinstance(o, uuid.UUID):
            return str(o)
        else:
            return json.JSONEncoder.default(self, o)


def init(args):
    global solr_connection
    solr_connection = SolrConnection(args.solr)
    global solr_collection
    solr_collection = solr_connection[args.collection]
    global SOLR_UNIQUE_KEY
    SOLR_UNIQUE_KEY = args.solrIdField

    dc_policy = RoundRobinPolicy()
    token_policy = TokenAwarePolicy(dc_policy)

    if args.cassandraUsername and args.cassandraPassword:
        auth_provider = PlainTextAuthProvider(username=args.cassandraUsername, password=args.cassandraPassword)
    else:
        auth_provider = None

    global cassandra_cluster
    cassandra_cluster = Cluster(contact_points=args.cassandra, port=args.cassandraPort,
                                protocol_version=int(args.cassandraProtocolVersion),
                                load_balancing_policy=token_policy,
                                auth_provider=auth_provider)
    global cassandra_session
    cassandra_session = cassandra_cluster.connect(keyspace=args.cassandraKeyspace)

    global cassandra_table
    cassandra_table = args.cassandraTable

    global logger
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s"
    )

    logger = logging.getLogger(__name__)

    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)
    logging.getLogger().handlers[0].setFormatter(
        logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S"
        ))

    logging.getLogger('cassandra').setLevel(logging.CRITICAL)


def compare_page(args, start) -> Tuple[List, List, List, int]:
    se = SearchOptions()

    logger.debug(f'Running solr query from {start:,} to {start + args.rows:,}')

    se.commonparams.fl(SOLR_UNIQUE_KEY).q(args.q).start(start).rows(args.rows)

    query = solr_collection.search(se)
    docs = query.result.response.docs

    ids = [str(uuid.UUID(row[SOLR_UNIQUE_KEY])) for row in docs]

    statement = cassandra_session.prepare("SELECT tile_id FROM %s where tile_id=?" % cassandra_table)

    retries = 3

    extra = []

    is_retry = False

    while retries > 0:
        if is_retry:
            logger.debug('Retrying query with failed IDs')

        logger.debug(f'Starting Cassandra query for {len(ids):,} tiles')
        results = cassandra.concurrent.execute_concurrent_with_args(
            cassandra_session,
            statement,
            [(uuid.UUID(str(id)),) for id in ids],
            concurrency=10000,
            raise_on_first_error=False,
            results_generator=True
        )

        failed = []
        present = []

        logger.debug('Processing Cassandra results')
        for (success, result) in results:
            if not success:
                failed.append(str(result))
            else:
                rows = result.all()

                present.extend([str(row[0]) for row in rows])

        logger.debug(f'Finished processing Cassandra results: found {len(present):,} tiles')

        for id in present:
            try:
                ids.remove(id)
            except:
                extra.append(id)

        if len(failed) > 0:
            logger.warning(f'{len(failed)} queries failed, maybe retrying')
            retries -= 1
            is_retry = True
            sleep(5)
        else:
            break

    logger.debug('Page stats: \n' + json.dumps({
        'missing': len(ids),
        'extra': len(extra),
        'failed': len(failed),
        'total_checked': len(docs)
    }, indent=4))

    return ids, extra, failed, len(docs)


def do_comparison(args):
    missing_cassandra = []
    missing_solr = []
    failed = []

    se = SearchOptions()

    se.commonparams.rows(0).q(args.q)

    num_tiles = solr_collection.search(se).result.response.numFound

    logger.info(f'Found {num_tiles:,} tiles in Solr')

    limit = num_tiles

    if args.limit:
        limit = min(num_tiles, args.limit)

    if limit >= PROCEED_THRESHOLD:
        do_continue = input(f"There are a large number of tile IDs to check. Do you wish to proceed? [y]/n: ")

        while do_continue not in ['y', 'n', '']:
            do_continue = input(f"There are a large number of tile IDs to check. Do you wish to proceed? [y]/n: ")

        if do_continue == 'n':
            do_continue = input(f"Do you wish to proceed with a limit (300 thousand)? [y]/n: ")

            while do_continue not in ['y', 'n', '']:
                do_continue = input(f"Do you wish to proceed with a limit (300 thousand)? [y]/n: ")

            if do_continue == 'n':
                logger.info('Exiting...')
                exit(0)
            else:
                limit = PROCEED_THRESHOLD

    start = 0

    while start < limit:
        absent, extra, failed_queries, checked = compare_page(args, start)
        start += checked

        missing_cassandra.extend(absent)
        missing_solr.extend(extra)
        failed.extend(failed_queries)

    if len(missing_cassandra) > 0:
        logger.info(f'Found {len(missing_cassandra):,} tile IDs missing from Cassandra:\n' +
                    json.dumps(missing_cassandra, indent=4, cls=Encoder))
    else:
        logger.info('No tiles found missing from Cassandra')

    if len(missing_solr) > 0:
        logger.info(f'Found {len(missing_solr):,} tile IDs missing from Solr:\n' +
                    json.dumps(missing_solr, indent=4, cls=Encoder))
    else:
        logger.info('No tiles found missing from Solr')

    if len(failed) > 0:
        logger.info(f'There were {len(failed):,} Cassandra queries that failed:\n' +
                    json.dumps(failed, indent=4, cls=Encoder))
    else:
        logger.info('No Cassandra queries have failed')


def parse_args():
    parser = argparse.ArgumentParser(description='Ensure tiles in Solr exist in Cassandra',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--solr',
                        help='The url of the SOLR server.',
                        required=True,
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
                        required=True,
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
                        required=False)

    parser.add_argument('--cassandraPassword',
                        help='The password used to connect to Cassandra.',
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

    parser.add_argument('--limit',
                        help='Maximum number of IDs to check. Default is all tiles',
                        required=False,
                        dest='limit',
                        default=None,
                        type=int)

    parser.add_argument('-q',
                        help='Solr query string',
                        required=False,
                        dest='q',
                        default='*:*',
                        metavar='QUERY')

    parser.add_argument('-v', '--verbose', dest='verbose', action='store_true', help='Enable verbose output')

    return parser.parse_args()


def main():
    args = parse_args()
    init(args)
    do_comparison(args)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.error('Something went wrong!')
        logging.exception(e)

    if logger:
        logger.info('Exiting')
    else:
        logging.info('Exiting')
