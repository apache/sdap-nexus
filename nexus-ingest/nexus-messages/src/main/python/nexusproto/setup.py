"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
from setuptools import setup

__version__ = '0.4'

setup(
    name='nexusproto',
    version=__version__,
    url="https://github.jpl.nasa.gov/thuang/nexus",

    author="Team Nexus",

    description="Protobufs used when passing NEXUS messages across the wire.",

    packages=['nexusproto'],
    platforms='any',

    install_requires=[
        'protobuf'
    ],

    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
    ]
)
