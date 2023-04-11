import argparse
import json
import logging
from datetime import datetime
from typing import Tuple, List

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy
from dateutil import parser as du_parser
from dateutil.relativedelta import relativedelta
from six.moves import input
from tqdm import tqdm

logging.getLogger('webservice.NexusHandler').setLevel(logging.CRITICAL)

from webservice.algorithms.doms.DomsInitialization import DomsInitializer


logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def main(args, before, keep_completed, keep_failed, purge_all, recreate):
    log.info('Connecting to Cassandra cluster')

    dc_policy = RoundRobinPolicy()
    token_policy = TokenAwarePolicy(dc_policy)

    if args.username and args.password:
        auth_provider = PlainTextAuthProvider(username=args.username, password=args.password)
    else:
        auth_provider = None

    try:
        with Cluster(args.hosts,
                     port=int(args.port),
                     execution_profiles={
                         EXEC_PROFILE_DEFAULT: ExecutionProfile(
                             load_balancing_policy=token_policy,
                             request_timeout=60.0,
                         )
                     },
                     protocol_version=int(args.pv),
                     auth_provider=auth_provider) as cluster:
            session = cluster.connect(args.keyspace) if not recreate else cluster.connect()

            log.info('Connected successfully to Cassandra')

            if recreate:
                log.info('Recreating doms keyspace')
                create_keyspace(session, args.keyspace)
                exit(0)

            log.info('Determining the number of executions that will be dropped')

            execution_count, ids = count_executions(session, before, keep_completed, keep_failed, purge_all)

            do_continue = input(f'{execution_count:,} executions selected for deletion. Continue? [y]/n: ')

            while do_continue not in ['y', 'n', '']:
                do_continue = input(f'{execution_count:,} executions selected for deletion. Continue? [y]/n: ')

            if do_continue == 'n':
                exit(0)

            if purge_all:
                purge_all_data(session, args.keyspace)
            else:
                print(json.dumps([str(rid) for rid in ids], indent=4))
                for row_id in tqdm(ids):
                    delete_execution(session, row_id)
    except NoHostAvailable as ne:
        log.exception(ne)


def delete_execution(session, row_id):
    cql_data = session.prepare("""
    DELETE FROM doms_data WHERE execution_id=?;
    """)
    cql_execution_stats = session.prepare("""
    DELETE FROM doms_execution_stats WHERE execution_id=?;
    """)
    cql_params = session.prepare("""
    DELETE FROM doms_params WHERE execution_id=?;
    """)
    cql_executions = session.prepare("""
    DELETE FROM doms_executions WHERE id=?;
    """)

    session.execute(cql_data, (row_id,))
    session.execute(cql_execution_stats, (row_id,))
    session.execute(cql_params, (row_id,))
    session.execute(cql_executions, (row_id,))


def create_keyspace(session, keyspace):
    log.info('*** RUNNING DOMS INITIALIZATION ***')

    initializer = DomsInitializer()
    initializer.createKeyspace(session, keyspace)
    initializer.createTables(session)


def purge_all_data(session, keyspace):
    do_continue = input('You have selected to purge all data. This will drop and recreate the doms keyspace. '
                        'Continue? [y]/n: ')

    while do_continue not in ['y', 'n', '']:
        do_continue = input('You have selected to purge all data. This will drop and recreate the doms keyspace. '
                            'Continue? [y]/n: ')

    if do_continue == 'n':
        exit(0)

    cql = f"""
    drop keyspace {keyspace};
    """

    log.info('Executing keyspace drop')
    session.execute(cql, timeout=None)
    log.info(f'\'{keyspace}\' keyspace dropped. Recreating it now.')
    log.info('NOTE: If something goes wrong with keyspace recreation, rerun this utility with just \'--recreate-ks\''
             ' and the cassandra auth options')
    create_keyspace(session, keyspace)


def count_executions(session, before, keep_completed, keep_failed, purge_all) -> Tuple[int, List]:
    if purge_all:
        cql = """
        SELECT COUNT (id) FROM doms_executions;
        """

        return session.execute(cql).one().system_count_id, []
    elif before and not keep_failed:    # Drop nulls & all before
        # Cassandra doesn't allow for selecting null values, so we have to check them all manually

        cql = """
        SELECT * FROM doms_executions;
        """

        to_delete = []

        for row in session.execute(cql):
            if row.time_completed is None or row.time_completed <= before:
                to_delete.append(row.id)

        return len(to_delete), to_delete
    elif before and keep_failed:    # Drop all before but not nulls
        cql = """
                SELECT id FROM doms_executions WHERE time_completed<=? ALLOW FILTERING ;
                """

        query = session.prepare(cql)
        to_delete = []

        for row in session.execute(query, (before,)):
            to_delete.append(row.id)

        return len(to_delete), to_delete
    elif keep_completed:   # Only drop nulls
        # Cassandra doesn't allow for selecting null values, so we have to check them all manually

        cql = """
                SELECT * FROM doms_executions;
                """

        to_delete = []

        for row in session.execute(cql):
            if row.time_completed is None:
                to_delete.append(row.id)

        return len(to_delete), to_delete


def parse_args():
    parser = argparse.ArgumentParser(description='Purge DOMS data from Cassandra')

    cassandra_args = parser.add_argument_group('Cassandra args')
    purge_options = parser.add_argument_group('Purge options')

    cassandra_args.add_argument('--cassandra', help='The hostname(s) or IP(s) of the Cassandra server(s).',
                                required=False,
                                default=['localhost'],
                                dest='hosts',
                                nargs='+',
                                metavar=('localhost', '127.0.0.101'))

    cassandra_args.add_argument('-k', '--cassandraKeyspace',
                                help='The Cassandra keyspace for DOMS data.',
                                default='doms',
                                dest='keyspace',
                                required=False,
                                metavar='DOMS_KEYSPACE')

    cassandra_args.add_argument('--cassandraPort',
                                help='The port used to connect to Cassandra.',
                                dest='port',
                                required=False,
                                default='9042')

    cassandra_args.add_argument('-u', '--cassandra-username',
                                dest='username', help='The username used to connect to Cassandra.',
                                required=True, metavar='USERNAME')

    cassandra_args.add_argument('-p', '--cassandra-password',
                                dest='password', help='The password used to connect to Cassandra.',
                                required=True, metavar='PASSWORD')

    cassandra_args.add_argument('--cassandraProtocolVersion',
                                help='The version of the Cassandra protocol the driver should use.',
                                required=False,
                                dest='pv',
                                choices=['1', '2', '3', '4', '5'],
                                default='3')

    time_before = purge_options.add_mutually_exclusive_group(required=True)

    time_before.add_argument('--before',
                             help='Date & time before which data will be purged',
                             type=du_parser.parse,
                             dest='before_dt',
                             metavar='DATETIME',
                             default=None)

    def num_months(s):
        v = int(s)

        if v <= 0:
            raise ValueError('--before-months must be >= 1')

        return v

    time_before.add_argument('--before-months',
                             help='Drop all data before n months ago',
                             type=num_months,
                             dest='before_mo',
                             metavar='MONTHS',
                             default=None)

    time_before.add_argument('--keep-completed',
                             help='Keep all completed executions (only purge failed executions)',
                             action='store_true',
                             dest='keep')

    time_before.add_argument('--all',
                             help='Purge ALL data (drops and re-creates keyspace)',
                             action='store_true',
                             dest='all')

    time_before.add_argument('--recreate-ks',
                             help=argparse.SUPPRESS,
                             action='store_true',
                             dest='recreate')

    purge_options.add_argument('--keep-failed',
                               help='Keep failed executions.',
                               action='store_true',
                               dest='keep_failed')

    args = parser.parse_args()

    # print(args)

    if args.recreate:
        return args, None, False, False, False, True

    if args.all:
        if args.keep_failed:
            raise ValueError('Mutually exclusive options (purge all & keep) selected')

        return args, None, False, False, True, False

    if args.keep and args.keep_failed:
        raise ValueError('--keep-completed and --keep-failed are set; this will have no effect')

    if args.keep:
        before = None
    elif args.before_dt:
        before = args.before_dt
    else:
        now = datetime.now()
        delta = relativedelta(months=-args.before_mo)
        before = now + delta

    return args, before, args.keep, args.keep_failed, False, False


if __name__ == '__main__':
    main(*parse_args())
