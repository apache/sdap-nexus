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
from typing import Dict, Literal
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
from webservice.NexusHandler import nexus_handler
from webservice.algorithms.NexusCalcHandler import NexusCalcHandler

logger = logging.getLogger(__name__)


class TomogramBaseClass(NexusCalcHandler):
    def __int__(self, tile_service_factory, slice_bounds: Dict[Literal['lat', 'lon', 'elevation'], slice], **kwargs):
        NexusCalcHandler.__init__(self, tile_service_factory)
        self.__slice_bounds = slice_bounds

        # slice bounds: dict relating dimension names to desired slicing
        # When dealing with multi-var tiles, optional parameter to pick variable to plot, otherwise issue warning and pick the first one


@nexus_handler
class LatitudeTomogramImpl(TomogramBaseClass):
    pass


@nexus_handler
class LongitudeTomogramImpl(TomogramBaseClass):
    pass


@nexus_handler
class ElevationTomogramImpl(TomogramBaseClass):
    pass
