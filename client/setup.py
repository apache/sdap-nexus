

from setuptools import setup, find_packages

__version__ = '1.0'

setup(
    name="nexuscli",
    version=__version__,
    packages=find_packages(),
    url="https://github.jpl.nasa.gov/thuang/nexus",

    author="Team Nexus",

    description="NEXUS Client Module",
    long_description=open('README.md').read(),

    platforms='any',

    install_requires=[
        'requests',
        'shapely',
        'numpy',
        'pytz'
    ],

    classifiers=[
        'Development Status :: 1 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.7',
    ]
)
