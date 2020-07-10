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
import ClimMapSpark
import CorrMapSpark
import DailyDifferenceAverageSpark
import HofMoellerSpark
import Matchup
import MaximaMinimaSpark
import NexusCalcSparkHandler
import TimeAvgMapSpark
import TimeSeriesSpark
import VarianceSpark


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
    except ImportError:
        pass

    try:
        import Matchup
    except ImportError:
        pass

    try:
        import TimeAvgMapSpark
    except ImportError:
        pass

    try:
        import VarianceSpark
    except ImportError:
        pass

    try:
        import MaximaMinimaSpark
    except ImportError:
        pass

    try:
        import TimeSeriesSpark
    except ImportError:
        pass

    try:
        import ClimMapSpark
    except ImportError:
        pass

    try:
        import DailyDifferenceAverageSpark
    except ImportError:
        pass

    try:
        import HofMoellerSpark
    except ImportError:
        pass


else:
    log.warn("pyspark not found. Skipping algorithms in %s" % os.path.dirname(__file__))
