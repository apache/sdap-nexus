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

import numpy as np
import math
import types

class ColorTable:

    def __init__(self, identifier, name, spec):
        self.identifier = identifier
        self.name = name

        # If the list of colors was passed in as a list of hex strings ("ABCDEFFF"), convert them to RGB values
        if isinstance(spec[0], types.StringType):
            spec = [[int(p[i:i+2], 16) for i in range(0, 6, 2)] for p in spec]

        self.spec = np.array(spec)

    def get_color(self, fraction):
        """
        Determines the color of a value given it's fraction within in a 0-1 range of the min/max and a colortable
        :param fraction: The floating-point value within a range 0 and 1
        :return: The color with values within the range of 0 and 255 as (red, green, blue, alpha)
        """

        index = fraction * (len(self.spec) - 1.0)
        low_index = int(math.floor(index))
        high_index = int(math.ceil(index))

        f = index - low_index
        low_color = self.spec[low_index]
        high_color = self.spec[high_index]

        rgb = high_color * f + low_color * (1.0 - f)
        rgb = np.append(rgb, 255)
        return rgb