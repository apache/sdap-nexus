import argparse
import configparser
import decimal
import json
import logging

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import (DCAwareRoundRobinPolicy, TokenAwarePolicy,
                                WhiteListRoundRobinPolicy)

try:
    logging.getLogger('webservice.NexusHandler').setLevel(logging.CRITICAL)
except:
    pass

from webservice.algorithms.doms.DomsInitialization import DomsInitializer

BATCH_SIZE = 1024
log = logging.getLogger(__name__)


class Encoder(json.JSONEncoder):
    def __init__(self, **args):
        json.JSONEncoder.__init__(self, **args)

    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        else:
            return json.JSONEncoder.default(self, obj)


def main():
    domsconfig = configparser.ConfigParser()
    domsconfig.read(DomsInitializer._get_config_files('domsconfig.ini'))

    parser = argparse.ArgumentParser()

    parser.add_argument('-u', '--cassandra-username',
                        dest='username', help='The username used to connect to Cassandra.',
                        required=True, metavar='USERNAME')

    parser.add_argument('-p', '--cassandra-password',
                        dest='password', help='The password used to connect to Cassandra.',
                        required=True, metavar='PASSWORD')

    parser.add_argument('--cassandra', help='The hostname(s) or IP(s) of the Cassandra server(s).',
                        required=False,
                        default=domsconfig.get("cassandra", "host"),
                        dest='hosts',
                        nargs='+',
                        metavar=('localhost', '127.0.0.101'))

    parser.add_argument('--cassandraPort',
                        help='The port used to connect to Cassandra.',
                        dest='port',
                        required=False,
                        default=domsconfig.get("cassandra", "port"))

    args = parser.parse_args()

    cassHost = args.hosts
    cassPort = args.port
    cassUsername = args.username
    cassPassword = args.password
    cassKeyspace = domsconfig.get("cassandra", "keyspace")
    cassDatacenter = domsconfig.get("cassandra", "local_datacenter")
    cassVersion = int(domsconfig.get("cassandra", "protocol_version"))
    cassPolicy = domsconfig.get("cassandra", "dc_policy")

    log.info("Cassandra Host(s): %s" % (cassHost))
    log.info("Cassandra Keyspace: %s" % (cassKeyspace))
    log.info("Cassandra Datacenter: %s" % (cassDatacenter))
    log.info("Cassandra Protocol Version: %s" % (cassVersion))
    log.info("Cassandra DC Policy: %s" % (cassPolicy))
    log.info("Cassandra Auth: %s : %s" % (cassUsername, cassPassword))

    if cassPolicy == 'DCAwareRoundRobinPolicy':
        dc_policy = DCAwareRoundRobinPolicy(cassDatacenter)
        token_policy = TokenAwarePolicy(dc_policy)
    elif cassPolicy == 'WhiteListRoundRobinPolicy':
        token_policy = WhiteListRoundRobinPolicy([cassHost])
    else:
        raise ValueError(cassPolicy)

    if cassUsername and cassPassword:
        auth_provider = PlainTextAuthProvider(username=cassUsername, password=cassPassword)
    else:
        auth_provider = None

    try:
        with Cluster([host for host in cassHost.split(',')],
                     port=int(cassPort),
                     execution_profiles={
                         EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=token_policy)
                     },
                     protocol_version=cassVersion,
                     auth_provider=auth_provider) as cluster:
            session = cluster.connect(cassKeyspace)

            cql = """
            alter table doms_data
                 add measurement_values_json text;
            """

            log.info('Creating measurement_values_json column')

            try:
                session.execute(cql)
            except:
                log.warning('measurement_values_json column creation failed; perhaps it already exists')

            cql = """
            alter table doms_data
                 add file_url text;
            """

            log.info('Creating file_url column')

            try:
                session.execute(cql)
            except:
                log.warning('file_url column creation failed; perhaps it already exists')

            for i in range(5):
                if not move_data(session):
                    if i < 4:
                        log.warning('Some move attempts failed; retrying')
                    else:
                        log.critical('Some move attempts failed; max retries exceeded')
                        exit(1)
                else:
                    break

            cql = """
            alter table doms_data
                drop measurement_values;
            """

            log.info('Dropping old measurement_values column')
            session.execute(cql)
    except NoHostAvailable as e:
        log.error("Unable to connect to Cassandra, Nexus will not be able to access local data ", e)
    except Exception as e:
        log.critical('An uncaught exception occurred')
        log.exception(e)
        exit(2)


def move_data(session):
    cql = """
        SELECT execution_id, is_primary, id, measurement_values FROM doms_data;
    """

    log.info('Fetching execution measurements')

    try:
        rows = session.execute(cql)
    except:
        log.warning('SELECT query failed; the measurement_values column may no longer exist')
        exit(0)

    update_params = []

    for row in rows:
        if row.measurement_values is not None:
            update_params.append((
                json.dumps(translate_values(dict(row.measurement_values)), indent=4, cls=Encoder),  # values
                row.execution_id,  # execution_id
                row.is_primary,  # is_primary
                row.id,  # id
            ))

    update_cql = """
        UPDATE doms_data
        SET measurement_values=null,
        measurement_values_json=?
        WHERE execution_id=? AND 
        is_primary=? AND
        id=?;
    """

    update_query = session.prepare(update_cql)
    query_batches = [update_params[i:i + BATCH_SIZE] for i in range(0, len(update_params), BATCH_SIZE)]
    move_successful = True

    n_entries = len(update_params)
    writing = 0

    log.info(f'Writing {n_entries} entries in JSON format')

    for batch in query_batches:
        futures = []
        writing += len(batch)
        log.info(f'Writing batch of {len(batch)} entries | ({writing}/{n_entries}) [{writing/n_entries*100:7.3f}%]')

        for entry in batch:
            futures.append(session.execute_async(update_query, entry))

        for future in futures:
            try:
                future.result()
            except Exception:
                move_successful = False

    log.info('Move attempt completed')
    return move_successful


def translate_values(values_dict):
    values = []

    for key in values_dict:
        values.append({
            "variable_name": "",
            "cf_variable_name": key,
            "variable_value": values_dict[key],
            "variable_unit": None
        })

    return values


if __name__ == '__main__':
    main()
