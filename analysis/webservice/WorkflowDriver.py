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

import argparse

from algorithms.MapFetchHandler import MapFetchCalcHandler


def start(args):
    dataset_shortname = args.ds
    granule_name = args.g
    prefix = args.p
    ct = args.ct
    _min = float(args.min)
    _max = float(args.max)
    width = int(args.w)
    height = int(args.h)
    interp = args.i
    time_interval = args.t

    map = MapFetchCalcHandler()
    map.generate(dataset_shortname, granule_name, prefix, ct, interp, _min, _max, width, height, time_interval)


def parse_args():
    parser = argparse.ArgumentParser(description='Automate NEXUS ingestion workflow',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--ds', '--datasetShortName',
                        help='The shortname of the dataset',
                        required=True)

    parser.add_argument('--g', '--granuleName',
                        help='The filename of the granule',
                        required=True)

    parser.add_argument('--p', '--prefix',
                        help='The desired filename prefix',
                        required=False)

    parser.add_argument('--ct', '--colorTable',
                        help='Identifier of a supported color table. DEFAULT: smap',
                        required=False)

    parser.add_argument('--i', '--interpolation',
                        help="Interpolation filter to use when rescaling image data. Can be 'near', 'lanczos', 'bilinear', or 'bicubic'",
                        required=False)

    parser.add_argument('--min', '--minimum',
                        help='Minimum value to use when computing color scales',
                        required=False)

    parser.add_argument('--max', '--maximum',
                        help='Maximum value to use when computing color scales',
                        required=False)

    parser.add_argument('--w', '--width',
                        help='Output image width (max: 8192). DEFAULT: 1024',
                        required=False)

    parser.add_argument('--h', '--height',
                        help='Output image height (max: 8192). DEFAULT: 512',
                        required=False)

    parser.add_argument('--t', '--timeInterval',
                        help="The time interval for imaging. Can be 'day' or 'month'. DEFAULT: month",
                        required=False)

    return parser.parse_args()


if __name__ == "__main__":
    the_args = parse_args()
    start(the_args)
