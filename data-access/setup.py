"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import setuptools
from Cython.Build import cythonize

__version__ = '0.32'

setuptools.setup(
    name="nexus-data-access",
    version=__version__,
    url="https://github.jpl.nasa.gov/thuang/nexus",

    author="Team Nexus",

    description="NEXUS API.",
    long_description=open('README.md').read(),

    packages=['nexustiles', 'nexustiles.model', 'nexustiles.dao'],
    package_data={'nexustiles': ['config/datastores.ini']},
    platforms='any',
    setup_requires=['cython'],
    install_requires=[
        'cassandra-driver==3.5.0',
        'solrpy==0.9.7',
        'requests',
        'nexusproto',
        'shapely'
    ],

    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
    ],

    ext_modules=cythonize(["**/*.pyx"]),
    zip_safe=False
)
