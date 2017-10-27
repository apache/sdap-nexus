"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import os
import logging

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
