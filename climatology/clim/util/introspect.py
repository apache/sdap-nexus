#!/bin/env python

"""
introspect.py -- A few introspection utilities, most importantly myArgs().
"""

import sys, os
from inspect import currentframe, getargvalues, getmodule


def callingFrame():
    """Return calling frame info."""
    return currentframe().f_back

def myArgs():
    """Return the arguments of the executing function (the caller of myArgs)."""
    return getargvalues(currentframe().f_back)[3]


def test():
    def func1(a, b, c):
        print currentframe()
        print getmodule(callingFrame())   # getmodule doesn't work on frame objects
        args = myArgs()
        print args
        return a + b + c

    return func1(1, 2, 3)


def main(args):
    print test()

if __name__ == '__main__':
    main(sys.argv[1:])
