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
import sys
import json
from datetime import datetime
from functools import reduce

import numpy as np
import numpy.ma as ma
import pkg_resources
from pytz import timezone, UTC
from shapely.geometry import MultiPolygon, box

from nexustiles.model.nexusmodel import Tile, BBox, TileStats, TileVariable
from nexustiles.nexustiles import NexusTileServiceException
from nexustiles.AbstractTileService import AbstractTileService

EPOCH = timezone('UTC').localize(datetime(1970, 1, 1))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)
logger = logging.getLogger("testing")


class ZarrBackend(AbstractTileService):
    def __init__(self, config):
        AbstractTileService.__init__(self)
        self.__config = config
