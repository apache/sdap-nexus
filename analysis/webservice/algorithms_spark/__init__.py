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


import logging
import os

log = logging.getLogger(__name__)


def module_exists(module_name):
    try:
        __import__(module_name)
    except ImportError:
        return False
    else:
        return True


if module_exists("pyspark"):
    try:
        import CorrMapSpark
    except ImportError as e:
        log.warning("Error importing CorrMapSpark", exc_info=e)

    try:
        import Matchup
    except ImportError as e:
        log.warning("Error importing Matchup", exc_info=e)

    try:
        import TimeAvgMapSpark
    except ImportError as e:
        log.warning("Error importing TimeAvgMapSpark", exc_info=e)

    try:
        import TimeSeriesSpark
    except ImportError as e:
        log.warning("Error importing TimeSeriesSpark", exc_info=e)

    try:
        import ClimMapSpark
    except ImportError as e:
        log.warning("Error importing ClimMapSpark", exc_info=e)

    try:
        import DailyDifferenceAverageSpark
    except ImportError as e:
        log.warning("Error importing DailyDifferenceAverageSpark", exc_info=e)

    try:
        import HofMoellerSpark
    except ImportError as e:
        log.warning("Error importing HofMoellerSpark", exc_info=e)


else:
    log.warn("pyspark not found. Skipping algorithms in %s" % os.path.dirname(__file__))
