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

#
# warn.py -- Utility routines to print warning & error messages like --
#            "module: error message"
#
try:  __file__
except: __file__ = 'warn.py'   # ensure __file__ is set for warning messages
                               # each module file will execute this code
import sys, os
from inspect import getmodule, currentframe

def echo(*s):
    """Stringify & join any number of args and print resulting string to stdout"""
    sys.stdout.write(' '.join(map(str, s)) + '\n')

def echon(*s):
    """Same as echo() except join with newlines."""
    sys.stdout.write('\n'.join(map(str, s)) + '\n')

def echo2(*s):
    """Stringify & join any number of args and print resulting string to stderr"""
    sys.stderr.write(' '.join(map(str, s)) + '\n')

def echo2n(*s):
    """Same as echo2() except join with newlines."""
    sys.stderr.write('\n'.join(map(str, s)) + '\n')

def moduleName(file):
    """Extract a module name from the python source file name, with appended ':'."""
    return os.path.splitext(os.path.split(file)[1])[0] + ":"


# Each module must define these functions so that the module name is the proper file.

def warn(*s):
    """Print a warning message to stderr, identifying the module it came from."""
    echo2(moduleName(__file__)+':', *s)

def die(ss, status=1):
    """Print a warning message to stderr, and die with a non-zero status value."""
    if type(ss) == str: ss = [ss]
    warn(*ss); sys.exit(status)

