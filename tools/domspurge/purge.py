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
import json
import logging
import sys
from datetime import datetime
from typing import Tuple, List

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy
from dateutil import parser as du_parser
from dateutil.relativedelta import relativedelta
from six.moves import input
from tqdm import tqdm

try:
    logging.getLogger('webservice.NexusHandler').setLevel(logging.CRITICAL)
    from webservice.algorithms.doms.DomsInitialization import DomsInitializer
except ImportError:
    from DomsInitialization import DomsInitializer

logging.basicConfig(level=logging.INFO, stream=sys.stdout)
log = logging.getLogger(__name__)
dry_run = False
non_interactive = False


def get_confirmation(prompt_string='Continue? [y]/n: '):
    if non_interactive:
        return True

    do_continue = input(prompt_string)

    while do_continue not in ['y', 'n', '']:
        do_continue = input(prompt_string)

    return do_continue != 'n'


def main(args, before, keep_completed, keep_failed, purge_all, recreate):
    log.info('Connecting to Cassandra cluster')

    dc_policy = RoundRobinPolicy()
    token_policy = TokenAwarePolicy(dc_policy)

    if args.username and args.password:
        auth_provider = PlainTextAuthProvider(username=args.username, password=args.password)
    else:
        auth_provider = None

    contact_points = []

    for host_list in args.hosts:
        contact_points.extend(host_list.split(','))

    try:
        with Cluster(contact_points,
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

            if dry_run:
                if execution_count == 0:
                    log.info('No executions will be deleted with the provided criteria')
                elif purge_all:
                    log.info(f'The \'{args.keyspace}\' keyspace will be dropped then recreated w/ all needed tables')
                else:
                    log.info(f'The following executions would be deleted: \n'
                             f'{json.dumps([str(rid) for rid in ids], indent=4)} \n'
                             f'Total: {len(ids):,}')

                exit(0)

            if execution_count == 0 and not purge_all:
                log.info('No executions will be deleted with the provided criteria')
                exit(0)
            elif execution_count == 0 and purge_all:
                if not get_confirmation('No executions will be deleted with the provided criteria. Do you still wish '
                                        f'to drop & recreate the \'{args.keyspace}\' keyspace? [y]/n: '):
                    exit(0)
            else:
                if not get_confirmation(f'{execution_count:,} executions selected for deletion. Continue? [y]/n: '):
                    exit(0)

            if purge_all:
                purge_all_data(session, args.keyspace)
            else:
                for row_id in tqdm(ids, ascii=True, desc='Executions deleted', ncols=80, unit='Execution'):
                    delete_execution(session, row_id)

                log.info(f'Successfully deleted the following executions: \n'
                         f'{json.dumps([str(rid) for rid in ids], indent=4)} \n'
                         f'Total: {len(ids):,}')
    except NoHostAvailable as ne:
        log.exception(ne)
        exit(1)


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
    if not get_confirmation(f'You have selected to purge all data. This will drop and recreate the \'{keyspace}\' '
                            'keyspace. Continue? [y]/n: '):
        exit(0)

    cql = f"""
    drop keyspace {keyspace};
    """

    log.info('Executing keyspace drop')
    log.info('NOTE: If something goes wrong with keyspace recreation, rerun this utility with just \'--recreate-ks\''
             ' and the cassandra auth options')
    session.execute(cql, timeout=None)
    log.info(f'\'{keyspace}\' keyspace dropped. Recreating it now.')
    create_keyspace(session, keyspace)


def count_executions(session, before, keep_completed, keep_failed, purge_all) -> Tuple[int, List]:
    if purge_all:
        cql = """
        SELECT COUNT (id) FROM doms_executions;
        """

        return session.execute(cql).one().system_count_id, []
    elif before and not keep_failed:    # Drop nulls & all before
        # Cassandra doesn't allow for selecting null values, so we have to check them all manually

        log.info(f'Counting executions before {before} including uncompleted executions')

        cql = """
        SELECT * FROM doms_executions;
        """

        to_delete = []

        for row in session.execute(cql):
            if (row.time_completed is None and row.time_started <= before) or \
               (row.time_completed is not None and row.time_completed <= before):
                to_delete.append(row.id)

        return len(to_delete), to_delete
    elif before and keep_failed:    # Drop all before but not nulls
        log.info(f'Counting executions before {before} excluding uncompleted executions')

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

        log.info(f'Counting ALL uncompleted executions')

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
                             help='Date & time before which data will be purged. Time entered should be UTC. Do not '
                                  'specify timezone.',
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

    parser.add_argument('--dry-run',
                        help='Only print the execution ids to be deleted / DB operations to be performed. Do not '
                             'actually alter the DB',
                        action='store_true',
                        dest='dry_run')

    parser.add_argument('-y', '--yes',
                        help='Do not ask for confirmation.',
                        action='store_true',
                        dest='yes')

    args = parser.parse_args()

    global dry_run
    global non_interactive
    dry_run = args.dry_run
    non_interactive = args.yes

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
        now = datetime.utcnow()
        delta = relativedelta(months=-args.before_mo)
        before = now + delta

    return args, before, args.keep, args.keep_failed, False, False


if __name__ == '__main__':
    try:
        main(*parse_args())
    except Exception as e:
        log.error('An unexpected error occurred...')
        log.exception(e)
        exit(-1)
