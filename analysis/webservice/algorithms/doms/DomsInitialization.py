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


import configparser
import logging

import pkg_resources

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.cluster import NoHostAvailable
from cassandra.policies import (DCAwareRoundRobinPolicy, TokenAwarePolicy,
                                WhiteListRoundRobinPolicy)
from webservice.NexusHandler import nexus_initializer


@nexus_initializer
class DomsInitializer:
    def __init__(self):
        pass

    def init(self, config):
        self.log = logging.getLogger(__name__)
        self.log.info("*** STARTING DOMS INITIALIZATION ***")

        domsconfig = configparser.SafeConfigParser()
        domsconfig.read(DomsInitializer._get_config_files('domsconfig.ini'))
        domsconfig = self.override_config(domsconfig, config)

        cassHost = domsconfig.get("cassandra", "host")
        cassPort = domsconfig.get("cassandra", "port")
        cassUsername = domsconfig.get("cassandra", "username")
        cassPassword = domsconfig.get("cassandra", "password")
        cassKeyspace = domsconfig.get("cassandra", "keyspace")
        cassDatacenter = domsconfig.get("cassandra", "local_datacenter")
        cassVersion = int(domsconfig.get("cassandra", "protocol_version"))
        cassPolicy = domsconfig.get("cassandra", "dc_policy")
        try:
            cassCreateKeyspaceGranted = domsconfig.get("cassandra", "create_keyspace_granted")
        except configparser.NoOptionError:
            cassCreateKeyspaceGranted = "True"

        self.log.info("Cassandra Host(s): %s" % (cassHost))
        self.log.info("Cassandra Keyspace: %s" % (cassKeyspace))
        self.log.info("Cassandra Datacenter: %s" % (cassDatacenter))
        self.log.info("Cassandra Protocol Version: %s" % (cassVersion))
        self.log.info("Cassandra DC Policy: %s" % (cassPolicy))

        if cassPolicy == 'DCAwareRoundRobinPolicy':
            dc_policy = DCAwareRoundRobinPolicy(cassDatacenter)
            token_policy = TokenAwarePolicy(dc_policy)
        elif cassPolicy == 'WhiteListRoundRobinPolicy':
            token_policy = WhiteListRoundRobinPolicy([cassHost])

        if cassUsername and cassPassword:
            auth_provider = PlainTextAuthProvider(username=cassUsername, password=cassPassword)
        else:
            auth_provider = None

        try:
            with Cluster([host for host in cassHost.split(',')],
                         port=int(cassPort),
                         load_balancing_policy=token_policy,
                         protocol_version=cassVersion,
                         auth_provider=auth_provider) as cluster:
                session = cluster.connect()

                if cassCreateKeyspaceGranted in ["True", "true"]:
                    self.createKeyspace(session, cassKeyspace)
                else:
                    session.set_keyspace(cassKeyspace)

                self.createTables(session)

        except NoHostAvailable as e:
            self.log.error("Unable to connect to Cassandra, Nexus will not be able to access local data ", e)


    def override_config(self, first, second):
        for section in second.sections():
            if first.has_section(section):  # only override preexisting section, ignores the other
                for option in second.options(section):
                    if second.get(section, option) is not None:
                        first.set(section, option, second.get(section, option))
        return first

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
              measurement_values_json text,
              depth decimal,
              file_url text,
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
