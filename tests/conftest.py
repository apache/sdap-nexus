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

import pytest


def pytest_addoption(parser):
    parser.addoption("--skip-matchup", action="store_true",
                     help="Skip matchup_spark test. (Only for script testing purposes)")
    parser.addoption('--matchup-warn-on-miscount', action='store_false',
                     help='Issue a warning for matchup tests if they return an unexpected number of matches; '
                          'otherwise fail')


def pytest_collection_modifyitems(config, items):
    skip_matchup = config.getoption("--skip-matchup")

    if skip_matchup:
        skip = pytest.mark.skip(reason="Manually skipped")
        for item in items:
            if "matchup_spark" in item.name:
                item.add_marker(skip)
