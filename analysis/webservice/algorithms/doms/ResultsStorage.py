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
import json
import logging
import uuid
from datetime import datetime
from time import sleep

import pkg_resources
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy
from pytz import UTC
from webservice.algorithms.doms.BaseDomsHandler import DomsEncoder
from webservice.webmodel import NexusProcessingException

BATCH_SIZE = 1024

logger = logging.getLogger(__name__)


class ResultInsertException(IOError):
    pass


class AbstractResultsContainer:
    def __init__(self, config=None):
        self._log = logging.getLogger(__name__)
        self._log.info("Creating DOMS Results Storage Instance")

        self._session = None
        self._config = configparser.RawConfigParser()
        self._config.read(AbstractResultsContainer._get_config_files('domsconfig.ini'))

        if config:
            self.override_config(config)
        else:
            print('Config NOT provided from params...')

    def __enter__(self):
        cassHost = self._config.get("cassandra", "host")
        cassKeyspace = self._config.get("cassandra", "keyspace")
        cassDatacenter = self._config.get("cassandra", "local_datacenter")
        cassVersion = int(self._config.get("cassandra", "protocol_version"))
        cassUsername = self._config.get("cassandra", "username")
        cassPassword = self._config.get("cassandra", "password")

        auth_provider = PlainTextAuthProvider(username=cassUsername, password=cassPassword)

        dc_policy = DCAwareRoundRobinPolicy(cassDatacenter)
        token_policy = TokenAwarePolicy(dc_policy)

        self._cluster = Cluster([host for host in cassHost.split(',')], load_balancing_policy=token_policy,
                                protocol_version=cassVersion, auth_provider=auth_provider)

        self._session = self._cluster.connect(cassKeyspace)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cluster.shutdown()

    def _parseDatetime(self, dtString):
        dt = datetime.strptime(dtString, "%Y-%m-%dT%H:%M:%SZ")
        epoch = datetime.utcfromtimestamp(0)
        time = (dt - epoch).total_seconds() * 1000.0
        return int(time)

    def override_config(self, config):
        for section in config.sections():
            if self._config.has_section(section):
                for option in config.options(section):
                    if config.get(section, option) is not None:
                        self._config.set(section, option, config.get(section, option))

    @staticmethod
    def _get_config_files(filename):
        log = logging.getLogger(__name__)
        candidates = []
        extensions = ['.default', '']
        for extension in extensions:
            try:
                candidate = pkg_resources.resource_filename(__name__, filename + extension)
                log.info('use config file {}'.format(filename + extension))
                candidates.append(candidate)
            except KeyError as ke:
                log.warning('configuration file {} not found'.format(filename + extension))
        return candidates


class ResultsStorage(AbstractResultsContainer):
    def __init__(self, config=None):
        AbstractResultsContainer.__init__(self, config)

    def insertInitialExecution(self, params, startTime, status, userEmail='', execution_id=None):
        """
        Initial insert into database for CDMS matchup request. This
        populates the execution and params table.
        """
        if isinstance(execution_id, str):
            execution_id = uuid.UUID(execution_id)

        execution_id = self.__insertExecution(execution_id, startTime, None, userEmail, status)
        self.__insertParams(execution_id, params)
        return execution_id

    def updateExecution(self, execution_id, completeTime, status, message, stats, results):
        if stats:
            self.__insertStats(execution_id, stats)
        if results:
            self.__insertResults(execution_id, results)
        self.__updateExecution(execution_id, completeTime, status, message)

    def __insertExecution(self, execution_id, startTime, completeTime, userEmail, status):
        """
        Insert new entry into execution table
        """
        if execution_id is None:
            execution_id = uuid.uuid4()

        cql = "INSERT INTO doms_executions (id, time_started, time_completed, user_email, status) VALUES (%s, %s, %s, %s, %s)"
        self._session.execute(cql, (execution_id, startTime, completeTime, userEmail, status))
        return execution_id

    def __updateExecution(self, execution_id, complete_time, status, message=None):
        # Only update the status if it's "running". Any other state is immutable.
        cql = "UPDATE doms_executions SET time_completed = %s, status = %s, message = %s WHERE id=%s IF status = 'running'"
        self._session.execute(cql, (complete_time, status, message, execution_id))
        return execution_id

    def __insertParams(self, execution_id, params):
        cql = """INSERT INTO doms_params
                    (execution_id, primary_dataset, matchup_datasets, depth_min, depth_max, time_tolerance, radius_tolerance, start_time, end_time, platforms, bounding_box, parameter)
                 VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        self._session.execute(cql, (execution_id,
                                    params["primary"],
                                    ",".join(params["matchup"]) if type(params["matchup"]) == list else params[
                                        "matchup"],
                                    params["depthMin"] if "depthMin" in list(params.keys()) else None,
                                    params["depthMax"] if "depthMax" in list(params.keys()) else None,
                                    int(params["timeTolerance"]),
                                    params["radiusTolerance"],
                                    params["startTime"],
                                    params["endTime"],
                                    params["platforms"],
                                    params["bbox"],
                                    params["parameter"]
                                    ))

    def __insertStats(self, execution_id, stats):
        cql = """
           INSERT INTO doms_execution_stats
                (execution_id, num_gridded_matched, num_gridded_checked, num_insitu_matched, num_insitu_checked, time_to_complete)
           VALUES
                (%s, %s, %s, %s, %s, %s)
        """
        self._session.execute(cql, (
            execution_id,
            stats["numPrimaryMatched"],
            None,
            stats["numSecondaryMatched"],
            None,
            stats["timeToComplete"]
        ))

    def __insertResults(self, execution_id, results):

        cql = """
           INSERT INTO doms_data
                (id, execution_id, value_id, primary_value_id, x, y, source_dataset, measurement_time, platform, device, measurement_values_json, is_primary, depth, file_url)
           VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        insertStatement = self._session.prepare(cql)

        inserts = []

        for result in results:
            # 'PRIMARY' arg since primary values cannot have primary_value_id be null anymore
            # Secondary matches are prepped recursively from this call
            inserts.extend(self.__prepare_result(execution_id, 'PRIMARY', result, insertStatement))

        for i in range(5):
            success, failed_entries = self.__insert_result_batches(inserts, insertStatement)

            if not success:
                if i < 4:
                    self._log.warning('Some write attempts failed; retrying')
                    inserts = failed_entries
                    sleep(10)
                else:
                    self._log.error('Some write attempts failed; max retries exceeded')
                    raise ResultInsertException('Some result inserts failed')
            else:
                break


    def __insert_result_batches(self, insert_params, insertStatement):
        query_batches = [insert_params[i:i + BATCH_SIZE] for i in range(0, len(insert_params), BATCH_SIZE)]
        move_successful = True

        n_inserts = len(insert_params)
        writing = 0

        failed = []

        self._log.info(f'Inserting {n_inserts} matchup entries in JSON format')

        for batch in query_batches:
            futures = []
            writing += len(batch)
            self._log.info(
                f'Writing batch of {len(batch)} matchup entries | ({writing}/{n_inserts}) [{writing / n_inserts * 100:7.3f}%]')

            for entry in batch:
                futures.append((entry, self._session.execute_async(insertStatement, entry)))

            for entry, future in futures:
                try:
                    future.result()
                except Exception:
                    move_successful = False
                    failed.append(entry)

        self._log.info('Result data write attempt completed')
        return move_successful, failed

    def __prepare_result(self, execution_id, primaryId, result, insertStatement):
        if 'primary' in result:
            data = result['primary']
        elif 'secondary' in result:
            data = result['secondary']
        else:
            data = []

        result_id = uuid.uuid4()

        insert_params = (
            result_id,
            execution_id,
            result["id"],
            primaryId,
            result["lon"],
            result["lat"],
            result["source"],
            result["time"],
            result["platform"] if "platform" in result else None,
            result["device"] if "device" in result else None,
            json.dumps(data, cls=DomsEncoder),
            1 if primaryId is 'PRIMARY' else 0,
            result["depth"],
            result['fileurl']
        )

        params_list = [insert_params]

        if "matches" in result:
            for match in result["matches"]:
                params_list.extend(self.__prepare_result(execution_id, result["id"], match, insertStatement))

        return params_list


class ResultsRetrieval(AbstractResultsContainer):
    def __init__(self, config=None):
        AbstractResultsContainer.__init__(self, config)

    def retrieveResults(self, execution_id, trim_data=False, page_num=1, page_size=1000):
        if isinstance(execution_id, str):
            execution_id = uuid.UUID(execution_id)

        params = self.retrieveParams(execution_id)
        stats = self.__retrieveStats(execution_id)
        data = self.__retrieveData(execution_id, trim_data=trim_data, page_num=page_num, page_size=page_size)
        return params, stats, data

    def __retrieveData(self, id, trim_data=False, page_num=1, page_size=1000):
        dataMap = self.__retrievePrimaryData(id, trim_data=trim_data, page_num=page_num, page_size=page_size)
        self.__enrichPrimaryDataWithMatches(id, dataMap, trim_data=trim_data)
        data = [dataMap[name] for name in dataMap]
        return data

    def __enrichPrimaryDataWithMatches(self, id, dataMap, trim_data=False):
        cql = f"SELECT * FROM doms_data where execution_id = {str(id)} and is_primary = false and primary_value_id = ?"
        statement = self._session.prepare(cql)

        primary_ids = list(dataMap.keys())

        logger.info(f'Getting secondary data for {len(primary_ids)} primaries of {str(id)}')

        for (success, rows) in execute_concurrent_with_args(
            self._session, statement, [(i,) for i in primary_ids], concurrency=50, results_generator=True
        ):
            for row in rows:
                entry = self.__rowToDataEntry(row, trim_data=trim_data)
                if row.primary_value_id in dataMap:
                    if not "matches" in dataMap[row.primary_value_id]:
                        dataMap[row.primary_value_id]["matches"] = []
                    dataMap[row.primary_value_id]["matches"].append(entry)

    def __retrievePrimaryData(self, id, trim_data=False, page_num=2, page_size=10):
        cql = "SELECT * FROM doms_data where execution_id = %s and is_primary = true limit %s"
        rows = self._session.execute(cql, [id, page_num * page_size])

        dataMap = {}
        for row in rows[(page_num-1)*page_size:page_num*page_size]:
            entry = self.__rowToDataEntry(row, trim_data=trim_data)
            dataMap[row.value_id] = entry
        return dataMap

    def __rowToDataEntry(self, row, trim_data=False):
        if trim_data:
            entry = {
                "lon": float(row.x),
                "lat": float(row.y),
                "source": row.source_dataset,
                "time": row.measurement_time.replace(tzinfo=UTC)
            }
        else:
            entry = {
                "platform": row.platform,
                "device": row.device,
                "lon": str(row.x),
                "lat": str(row.y),
                "point": f"Point({float(row.x):.3f} {float(row.y):.3f})",
                "time": row.measurement_time.replace(tzinfo=UTC),
                "depth": float(row.depth) if row.depth is not None else None,
                "fileurl": row.file_url if hasattr(row, 'file_url') else None,
                "id": row.value_id,
                "source": row.source_dataset,
            }

        # If doms_data uses the old schema, default to original behavior

        try:
            entry['primary' if row.is_primary else 'secondary'] = json.loads(row.measurement_values_json)
        except AttributeError:
            for key in row.measurement_values:
                value = float(row.measurement_values[key])
                entry[key] = value

        return entry

    def __retrieveStats(self, id):
        cql = "SELECT num_gridded_matched, num_insitu_matched, time_to_complete FROM doms_execution_stats where execution_id = %s limit 1"
        rows = self._session.execute(cql, (id,))
        for row in rows:
            stats = {
                "timeToComplete": row.time_to_complete,
                "numSecondaryMatched": row.num_insitu_matched,
                "numPrimaryMatched": row.num_gridded_matched,
            }
            return stats

        raise NexusProcessingException(reason=f'No stats found for id {str(id)}', code=404)

    def retrieveParams(self, id):
        cql = "SELECT * FROM doms_params where execution_id = %s limit 1"
        rows = self._session.execute(cql, (id,))
        for row in rows:
            matchup = row.matchup_datasets.split(",")

            if len(matchup) == 1:
                matchup = matchup[0]

            params = {
                "primary": row.primary_dataset,
                "matchup": matchup,
                "startTime": row.start_time.replace(tzinfo=UTC),
                "endTime": row.end_time.replace(tzinfo=UTC),
                "bbox": row.bounding_box,
                "timeTolerance": int(row.time_tolerance) if row.time_tolerance is not None else None,
                "radiusTolerance": float(row.radius_tolerance) if row.radius_tolerance is not None else None,
                "platforms": row.platforms,
                "parameter": row.parameter,
                "depthMin": float(row.depth_min) if row.depth_min is not None else None,
                "depthMax": float(row.depth_max) if row.depth_max is not None else None,
            }
            return params

        raise NexusProcessingException(reason=f'No params found for id {str(id)}', code=404)

    def retrieveExecution(self, execution_id):
        """
        Retrieve execution details from database.

        :param execution_id: Execution ID
        :return: execution status dictionary
        """

        cql = "SELECT * FROM doms_executions where id = %s limit 1"
        rows = self._session.execute(cql, (execution_id,))
        for row in rows:
            return {
                'status': row.status,
                'message': row.message,
                'timeCompleted': row.time_completed,
                'timeStarted': row.time_started
            }

        raise NexusProcessingException(reason=f'Execution not found with id {str(execution_id)}', code=404)
