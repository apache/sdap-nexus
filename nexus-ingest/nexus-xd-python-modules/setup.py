"""
Copyright (c) 2016 Jet Propulsion Laboratory,
California Institute of Technology.  All rights reserved
"""
import setuptools

__version__ = '0.1'

setuptools.setup(
    name="nexusxd",
    version=__version__,
    url="https://github.jpl.nasa.gov/thuang/nexus",

    author="Team Nexus",

    description="Python modules that can be used as part of an XD Stream for NEXUS ingest.",
    long_description=open('README.md').read(),

    packages=['nexusxd'],
    test_suite="tests",
    platforms='any',
    install_requires=[
        'pytz',
        'springxd',
        'nexusproto'
    ],

    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
    ]
)
