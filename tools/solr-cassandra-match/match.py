import argparse
import json
import logging
import uuid
from datetime import datetime
from time import sleep
from typing import List, Tuple

import cassandra.concurrent
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy
from six.moves import input
from solrcloudpy import SolrConnection, SearchOptions
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

solr_connection = None
solr_collection = None
SOLR_UNIQUE_KEY = None

cassandra_cluster = None
cassandra_session = None
cassandra_table = None

logger = logging

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

    global solr_connection
    solr_connection = SolrConnection(args.solr, timeout=60)
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
                                execution_profiles={
                                    EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=token_policy)
                                },
                                auth_provider=auth_provider)
    global cassandra_session
    cassandra_session = cassandra_cluster.connect(keyspace=args.cassandraKeyspace)

    global cassandra_table
    cassandra_table = args.cassandraTable


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=10, max=60))
def try_solr(collection, se):
    t_s = datetime.now()

    try:
        logger.debug('Starting Solr query')

        response = collection.search(se)

        t_e = datetime.now()
        elapsed = t_e - t_s
        logger.debug(f'Finished Solr query in {elapsed}')

        return response
    except Exception as e:
        t_e = datetime.now()
        elapsed = t_e - t_s

        logger.error(f"Solr query failed after {elapsed}")
        logger.exception(e)

        solr_connection.timeout *= 2

        raise


def compare_page(args, start, mark) -> Tuple[List, List, List, int, str]:
    se = SearchOptions()

    logger.info(f'Running solr query from {start:,} to {start + args.rows:,}')

    se.commonparams.fl(SOLR_UNIQUE_KEY).q(args.q).rows(args.rows).sort(f'{SOLR_UNIQUE_KEY} asc')

    se.commonparams.remove_param('cursorMark')
    se.commonparams.add_params(cursorMark=mark)

    query = try_solr(solr_collection, se)
    docs = query.result.response.docs

    try:
        next_mark = query.result.nextCursorMark
    except AttributeError:
        return [], [], [], 0, ''

    if next_mark == mark:
        return [], [], [], 0, next_mark

    ids = [str(uuid.UUID(row[SOLR_UNIQUE_KEY])) for row in docs]

    statement = cassandra_session.prepare("SELECT tile_id FROM %s where tile_id=?" % cassandra_table)

    retries = 3

    extra = []
    failed = []

    is_retry = False
    wait = 5

    while retries > 0:
        if is_retry:
            sleep(wait)
            wait += 10
            logger.info('Retrying query with failed IDs')

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

        for (success, result) in tqdm(results, total=len(ids), desc='Cassandra queries', unit='tile'):
            if not success:
                failed.append(str(result))
            else:
                rows = result.all()

                present.extend([str(row[0]) for row in rows])

                found = [str(row[0]) for row in rows]

                for id in found:
                    try:
                        ids.remove(id)
                    except:
                        extra.append(id)

        logger.info(f'Finished processing Cassandra results: found {len(present):,} tiles')

        if len(failed) > 0:
            logger.warning(f'{len(failed)} queries failed, maybe retrying')
            retries -= 1
            is_retry = True
        else:
            break

    logger.debug('Page stats: \n' + json.dumps({
        'missing': len(ids),
        'extra': len(extra),
        'failed': len(failed),
        'total_checked': len(docs)
    }, indent=4))

    return ids, extra, failed, len(docs), next_mark


def do_comparison(args):
    missing_cassandra = []
    missing_solr = []
    failed = []

    se = SearchOptions()

    se.commonparams.rows(0).q(args.q)

    logger.info('Querying Solr to see how many tiles we need to check...')

    num_tiles = try_solr(solr_collection, se).result.response.numFound

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

    mark = '*'

    while True:
        absent, extra, failed_queries, checked, next_mark = compare_page(args, start, mark)
        start += checked

        if next_mark == mark:
            break
        else:
            mark = next_mark

        missing_cassandra.extend(absent)
        missing_solr.extend(extra)
        failed.extend(failed_queries)

        if start >= limit:
            break

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

    parser.add_argument('--limit',
                        help='Maximum number of IDs to check. Default is all tiles. Enforcement is currently loose; '
                             'will not run Solr queries past the limit but will check the full query. Eg. If limit is '
                             '750,000 and solr-rows is 200,000, the first 800,000 tiles will be checked',
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
    start = datetime.now()

    try:
        main()
    except Exception as e:
        logging.error('Something went wrong!')
        logging.exception(e)

    if logger:
        logger.info(f'Exiting. Run time = {datetime.now() - start}')
    else:
        logging.info(f'Exiting. Run time = {datetime.now() - start}')
