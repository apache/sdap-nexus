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

import json
from datetime import datetime
from decimal import Decimal

import numpy as np


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        """If input object is an ndarray it will be converted into a dict
        holding dtype, shape and the data, base64 encoded.
        """
        numpy_types = (
            np.bool_,
            # np.bytes_, -- python `bytes` class is not json serializable
            # np.complex64,  -- python `complex` class is not json serializable
            # np.complex128,  -- python `complex` class is not json serializable
            # np.complex256,  -- python `complex` class is not json serializable
            # np.datetime64,  -- python `datetime.datetime` class is not json serializable
            np.float16,
            np.float32,
            np.float64,
            # np.float128,  -- special handling below
            np.int8,
            np.int16,
            np.int32,
            np.int64,
            # np.object_  -- should already be evaluated as python native
            np.str_,
            np.uint8,
            np.uint16,
            np.uint32,
            np.uint64,
            np.void,
        )
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, numpy_types):
            return obj.item()
        elif isinstance(obj, np.float128):
            return obj.astype(np.float64).item()
        elif isinstance(obj, Decimal):
            return str(obj)
        elif isinstance(obj, datetime):
            return str(obj)
        elif obj is np.ma.masked:
            return str(np.NaN)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)