#!/usr/bin/env python3
from argparse import ArgumentParser
import os
import re

version_regex = re.compile('(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)((a(?P<alpha_num>\d+))|(-(?P<commit>.*)))?')

def main():
    parser = ArgumentParser()
    parser.add_argument('file')
    parser.add_argument('--phase', default='current')
    parser.add_argument('--value', default='auto')

    args = parser.parse_args()

    version_file = open(args.file, 'r+')
    version_contents = version_file.read()

    current_version = version_regex.search(version_contents)
    if current_version == None:
        print('Version not found in file')
        exit(1)

    if args.phase == 'current':
        print(current_version.group(0))
        return

    new_version = bump_version(current_version, args.phase, args.value)
    version_contents, _ = version_regex.subn(new_version, version_contents, count=1)

    print(new_version)

    version_file.seek(0)
    version_file.write(version_contents)
    version_file.truncate()
    version_file.close()

def bump_version(version, phase, value):
    major = int(version.group('major'))
    minor = int(version.group('minor'))
    patch = int(version.group('patch'))

    if phase == 'pre-alpha':
        if value == 'auto':
            raise Exception('value cannot be auto on pre-alpha')

        return f'{major}.{minor}.{patch}-{value}'
    elif phase == 'alpha':
        alpha_num = 0 if version.group('alpha_num') == None else int(version.group('alpha_num')) + 1

        return f'{major}.{minor}.{patch}a{alpha_num}'
    elif phase == 'patch':
        patch = patch + 1 if value == 'auto' else value

        return f'{major}.{minor}.{patch}'
    elif phase == 'minor':
        minor = minor + 1 if value == 'auto' else value
        patch = 0

        return f'{major}.{minor}.{patch}'
    elif phase == 'major':
        major = major + 1 if value == 'auto' else value
        minor = 0
        patch = 0

        return f'{major}.{minor}.{patch}'


if __name__ == '__main__':
    main()