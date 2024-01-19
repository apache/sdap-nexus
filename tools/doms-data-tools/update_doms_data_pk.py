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

"""
Script to transition doms.doms_data table to new schema with a primary key that will
enable faster execution retrieval for large matchOnce=false matchups. Due to the nature
of Cassandra, this will necessitate creating a temporary table, copying the data over,
dropping the old table, recreating the table with the adjusted schema, copying the data
back, and dropping the temporary table. This script does those steps, with the added
option to stop after the initial copy for testing purposes.
"""

import argparse
import logging
import sys

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy, TokenAwarePolicy

BATCH_SIZE = 10000
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s',
    stream=sys.stdout
)
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-u', '--cassandra-username',
        help='The username used to connect to Cassandra.',
        dest='username',
        required=False,
        default='cassandra',
        metavar='USERNAME'
    )

    parser.add_argument(
        '-p', '--cassandra-password',
        dest='password',
        help='The password used to connect to Cassandra.',
        required=False,
        default='cassandra',
        metavar='PASSWORD'
    )

    parser.add_argument(
        '--cassandra',
        help='The hostname(s) or IP(s) of the Cassandra server(s).',
        required=False,
        default=['localhost'],
        dest='hosts',
        nargs='+',
        metavar=('localhost', '127.0.0.101')
    )

    parser.add_argument(
        '--cassandraPort',
        help='The port used to connect to Cassandra.',
        dest='port',
        required=False,
        default=9042,
        type=int
    )

    parser.add_argument(
        '--action',
        help='Copy or move',
        dest='action',
        required=False,
        default='move',
        choices=['move', 'copy']
    )

    args = parser.parse_args()

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
                     protocol_version=3,
                     auth_provider=auth_provider) as cluster:

            session = cluster.connect('doms')

            log.info('Connected successfully to Cassandra')

            cql = """
            CREATE TABLE IF NOT EXISTS doms_data_temp (
              id uuid,
              execution_id uuid,
              value_id text,
              primary_value_id text,
              is_primary boolean,
              x decimal,
              y decimal,
              source_dataset text,
              measurement_time timestamp,
              platform text,
              device text,
              measurement_values_json text,
              depth decimal,
              file_url text,
              PRIMARY KEY ((execution_id, is_primary), primary_value_id, id)
            );
            """

            log.info('Creating temp data table')

            session.execute(cql)

            def move_table(src_t, dst_t, can_delete=True):
                move_rows = []

                cql = f"""
                SELECT * FROM {src_t};
                """

                data_rows = session.execute(cql)

                insert_cql = f"""
                INSERT INTO {dst_t}
                    (id, execution_id, value_id, primary_value_id, x, y, source_dataset, measurement_time, platform, device, measurement_values_json, is_primary, depth, file_url)
               VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

                insert_statement = session.prepare(insert_cql)

                n_moved = 0

                def do_move(rows):
                    remaining_rows = rows
                    failed = []
                    futures = []

                    log.info(f'Inserting {len(rows):,} rows to {dst_t}')

                    while len(remaining_rows) > 0:
                        for entry in rows:
                            futures.append((entry, session.execute_async(insert_statement, entry)))

                        for entry, future in futures:
                            try:
                                future.result()
                            except Exception:
                                failed.append(entry)

                        if len(failed) > 0:
                            remaining_rows = failed
                            failed = []

                            log.warning(f'Need to retry {len(remaining_rows):,} inserts')
                        else:
                            remaining_rows = []

                        return len(rows)

                for row in data_rows:
                    pvid = row.primary_value_id

                    if pvid is None:
                        pvid = 'PRIMARY'

                    move_rows.append(
                        (
                            row.id,
                            row.execution_id,
                            row.value_id,
                            pvid,
                            row.x,
                            row.y,
                            row.source_dataset,
                            row.measurement_time,
                            row.platform,
                            row.device,
                            row.measurement_values_json,
                            row.is_primary,
                            row.depth,
                            row.file_url
                        )
                    )

                    if len(move_rows) >= BATCH_SIZE:
                        n_moved += do_move(move_rows)
                        log.info(f'Moved {n_moved:,} rows so far')
                        move_rows = []

                if len(move_rows) > 0:
                    n_moved += do_move(move_rows)
                    log.info(f'Moved {n_moved:,} rows so far')

            log.info('Copying data to temp table')

            move_table('doms_data', 'doms_data_temp')

            if args.action == 'move':
                cql = """
                DROP TABLE doms_data;
                """

                log.info('Dropping old table')

                session.execute(cql)

                cql = """
                            CREATE TABLE doms_data (
                              id uuid,
                              execution_id uuid,
                              value_id text,
                              primary_value_id text,
                              is_primary boolean,
                              x decimal,
                              y decimal,
                              source_dataset text,
                              measurement_time timestamp,
                              platform text,
                              device text,
                              measurement_values_json text,
                              depth decimal,
                              file_url text,
                              PRIMARY KEY ((execution_id, is_primary), primary_value_id, id)
                            );
                            """

                log.info('Creating data table with corrected schema')

                session.execute(cql)

                log.info('Copying data back')

                move_table('doms_data_temp', 'doms_data', False)

                cql = """
                        DROP TABLE doms_data_temp;
                        """

                log.info('Dropping temp table')

                session.execute(cql)

            log.info('Disconnecting from Cassandra')
            session.shutdown()

        log.info('Done')
    except NoHostAvailable as ne:
        log.exception(ne)
        exit(1)


if __name__ == '__main__':
    main()
