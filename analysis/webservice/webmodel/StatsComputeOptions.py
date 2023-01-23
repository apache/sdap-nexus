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

class StatsComputeOptions(object):
    def __init__(self):
        pass

    def get_apply_seasonal_cycle_filter(self, default="false"):
        raise Exception("Please implement")

    def get_max_lat(self, default=90.0):
        raise Exception("Please implement")

    def get_min_lat(self, default=-90.0):
        raise Exception("Please implement")

    def get_max_lon(self, default=180):
        raise Exception("Please implement")

    def get_min_lon(self, default=-180):
        raise Exception("Please implement")

    def get_dataset(self):
        raise Exception("Please implement")

    def get_environment(self):
        raise Exception("Please implement")

    def get_start_time(self):
        raise Exception("Please implement")

    def get_end_time(self):
        raise Exception("Please implement")

    def get_start_year(self):
        raise Exception("Please implement")

    def get_end_year(self):
        raise Exception("Please implement")

    def get_clim_month(self):
        raise Exception("Please implement")

    def get_start_row(self):
        raise Exception("Please implement")

    def get_end_row(self):
        raise Exception("Please implement")

    def get_content_type(self):
        raise Exception("Please implement")

    def get_apply_low_pass_filter(self, default=False):
        raise Exception("Please implement")

    def get_low_pass_low_cut(self, default=12):
        raise Exception("Please implement")

    def get_low_pass_order(self, default=9):
        raise Exception("Please implement")

    def get_plot_series(self, default="mean"):
        raise Exception("Please implement")

    def get_plot_type(self, default="default"):
        raise Exception("Please implement")

    def get_nparts(self):
        raise Exception("Please implement")