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



import ConfigParser
import logging

import pkg_resources
from cassandra.cluster import Cluster
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy, WhiteListRoundRobinPolicy

from webservice.NexusHandler import nexus_initializer

@nexus_initializer
class DomsInitializer:
    def __init__(self):
        pass

    def init(self, config):
        log = logging.getLogger(__name__)
        log.info("*** STARTING DOMS INITIALIZATION ***")

        domsconfig = ConfigParser.SafeConfigParser()
        domsconfig.read(DomsInitializer._get_config_files('domsconfig.ini'))

        cassHost = domsconfig.get("cassandra", "host")
        cassPort = domsconfig.get("cassandra", "port")
        cassKeyspace = domsconfig.get("cassandra", "keyspace")
        cassDatacenter = domsconfig.get("cassandra", "local_datacenter")
        cassVersion = int(domsconfig.get("cassandra", "protocol_version"))
        cassPolicy = domsconfig.get("cassandra", "dc_policy")

        log.info("Cassandra Host(s): %s" % (cassHost))
        log.info("Cassandra Keyspace: %s" % (cassKeyspace))
        log.info("Cassandra Datacenter: %s" % (cassDatacenter))
        log.info("Cassandra Protocol Version: %s" % (cassVersion))
        log.info("Cassandra DC Policy: %s" % (cassPolicy))

        if cassPolicy == 'DCAwareRoundRobinPolicy':
            dc_policy = DCAwareRoundRobinPolicy(cassDatacenter)
        elif cassPolicy == 'WhiteListRoundRobinPolicy':
            dc_policy = WhiteListRoundRobinPolicy([cassHost])
        token_policy = TokenAwarePolicy(dc_policy)

        with Cluster([host for host in cassHost.split(',')], port=int(cassPort), load_balancing_policy=token_policy,
                     protocol_version=cassVersion) as cluster:
            session = cluster.connect()

            self.createKeyspace(session, cassKeyspace)
            self.createTables(session)

    def createKeyspace(self, session, cassKeyspace):
        log = logging.getLogger(__name__)
        log.info("Verifying DOMS keyspace '%s'" % cassKeyspace)
        session.execute(
            "CREATE KEYSPACE IF NOT EXISTS %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };" % cassKeyspace)
        session.set_keyspace(cassKeyspace)

    def createTables(self, session):
        log = logging.getLogger(__name__)
        log.info("Verifying DOMS tables")
        self.createDomsExecutionsTable(session)
        self.createDomsParamsTable(session)
        self.createDomsDataTable(session)
        self.createDomsExecutionStatsTable(session)

    def createDomsExecutionsTable(self, session):
        log = logging.getLogger(__name__)
        log.info("Verifying doms_executions table")
        cql = """
            CREATE TABLE IF NOT EXISTS doms_executions (
              id uuid PRIMARY KEY,
              time_started timestamp,
              time_completed timestamp,
              user_email text
            );
                """
        session.execute(cql)

    def createDomsParamsTable(self, session):
        log = logging.getLogger(__name__)
        log.info("Verifying doms_params table")
        cql = """
            CREATE TABLE IF NOT EXISTS doms_params (
              execution_id uuid PRIMARY KEY,
              primary_dataset text,
              matchup_datasets text,
              depth_tolerance decimal,
              depth_min decimal,
              depth_max decimal,
              time_tolerance int,
              radius_tolerance decimal,
              start_time timestamp,
              end_time timestamp,
              platforms text,
              bounding_box text,
              parameter text
            );
        """
        session.execute(cql)

    def createDomsDataTable(self, session):
        log = logging.getLogger(__name__)
        log.info("Verifying doms_data table")
        cql = """
            CREATE TABLE IF NOT EXISTS doms_data (
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
              measurement_values map<text, decimal>,
              PRIMARY KEY (execution_id, is_primary, id)
            );
        """
        session.execute(cql)

    def createDomsExecutionStatsTable(self, session):
        log = logging.getLogger(__name__)
        log.info("Verifying doms_execuction_stats table")
        cql = """
            CREATE TABLE IF NOT EXISTS doms_execution_stats (
              execution_id uuid PRIMARY KEY,
              num_gridded_matched int,
              num_gridded_checked int,
              num_insitu_matched int,
              num_insitu_checked int,
              time_to_complete int
            );
        """
        session.execute(cql)

    @staticmethod
    def _get_config_files(filename):
        log = logging.getLogger(__name__)
        candidates = []
        extensions = ['.default', '']
        for extension in extensions:
            try:
                candidate = pkg_resources.resource_filename(__name__, filename + extension)
                candidates.append(candidate)
            except KeyError as ke:
                log.warning('configuration file {} not found'.format(filename + extension))

        return candidates
