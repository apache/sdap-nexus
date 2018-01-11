"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import setuptools

__version__ = '1.5'

setuptools.setup(
    name="nexusanalysis",
    version=__version__,
    url="https://github.jpl.nasa.gov/thuang/nexus",

    author="Team Nexus",

    description="NEXUS API.",
    long_description=open('README.md').read(),

    packages=['webservice', 'webservice.algorithms', 'webservice.algorithms.doms', 'webservice.algorithms_spark'],
    package_data={'webservice': ['config/web.ini', 'config/algorithms.ini'],
                  'webservice.algorithms.doms': ['domsconfig.ini']},
    data_files=[
        ('static', ['static/index.html'])
    ],
    platforms='any',

    install_requires=[
        'nexus-data-access',
        'tornado',
        'singledispatch',
        'pytz',
        'cython',
        'requests',
        'utm',
        'shapely',
        'mock',
        'backports.functools-lru-cache==1.3',
        'netcdf4',
        'boto3',
        'pyproj==1.9.5.1',
        'pillow==5.0.0'
    ],

    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
    ]
)
