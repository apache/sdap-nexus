#!/bin/bash
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

python /domspurge/purge.py \
  $([[ ! -z "$CASSANDRA_CONTACT_POINTS" ]] && echo --cassandra=$CASSANDRA_CONTACT_POINTS) \
  $([[ ! -z "$CASSANDRA_PORT" ]] && echo --cassandraPort=$CASSANDRA_PORT) \
  $([[ ! -z "$CASSANDRA_KEYSPACE" ]] && echo --cassandraKeyspace=$CASSANDRA_KEYSPACE) \
  $([[ ! -z "$CASSANDRA_USERNAME" ]] && echo --cassandra-username=$CASSANDRA_USERNAME) \
  $([[ ! -z "$CASSANDRA_PASSWORD" ]] && echo --cassandra-password=$CASSANDRA_PASSWORD) \
  $([[ ! -z "$CASSANDRA_PROTOCOL" ]] && echo --cassandraProtocolVersion=$CASSANDRA_PROTOCOL) \
  $([[ ! -z "$YES" ]] && echo -y) \
  $([[ ! -z "$DRY_RUN" ]] && echo --dry-run) \
  $([[ ! -z "$BEFORE" ]] && echo --before=$BEFORE) \
  $([[ ! -z "$BEFORE_MONTHS" ]] && echo --before-months=$BEFORE_MONTHS) \
  $([[ ! -z "$KEEP_COMPLETED" ]] && echo --keep-completed) \
  $([[ ! -z "$KEEP_FAILED" ]] && echo --keep-failed) \
  $([[ ! -z "$PURGE_ALL" ]] && echo --all)
