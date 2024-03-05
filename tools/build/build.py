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
import os
import shutil
import subprocess
import tempfile
from tenacity import retry, stop_after_attempt, wait_fixed
from urllib.parse import urljoin

import requests

DOCKER = shutil.which('docker')

NEXUS_TARBALL = 'apache-sdap-nexus-{}-incubating-src.tar.gz'
INGESTER_TARBALL = 'apache-sdap-ingester-{}-incubating-src.tar.gz'

NEXUS_DIR = 'apache-sdap-nexus-{}-incubating-src'
INGESTER_DIR = 'apache-sdap-ingester-{}-incubating-src'

if DOCKER is None:
    raise OSError('docker command could not be found in PATH')


def build_cmd(tag, context, dockerfile='', cache=True):
    command = [DOCKER, 'build', context]

    if dockerfile != '':
        command.extend(['-f', os.path.join(context, dockerfile)])

    command.extend(['-t', tag])

    if not cache:
        command.append('--no-cache')

    return command


@retry(stop=stop_after_attempt(2), wait=wait_fixed(2))
def run(cmd, suppress_output=False, err_on_fail=True):
    stdout = subprocess.DEVNULL if suppress_output else None

    p = subprocess.Popen(cmd, stdout=stdout, stderr=subprocess.STDOUT)

    p.wait()

    if err_on_fail and p.returncode != 0:
        raise OSError(f'Subprocess returned nonzero: {p.returncode}')


def yes_no(prompt):
    do_continue = input(prompt).lower()

    while do_continue not in ['', 'y', 'n']:
        do_continue = input(prompt).lower()

    return do_continue in ['', 'y']


def choice_prompt(prompt: str, choices: list, default: str = None) -> str:
    assert len(choices) > 0

    if len(choices) == 1:
        return choices[0]

    valid_choices = [str(i) for i in range(len(choices))]

    if default is not None:
        assert default in choices
        valid_choices.append('')

    print(prompt)

    choice = None

    while choice not in valid_choices:
        for i, c in enumerate(choices):
            print('[{:2d}] {}-> {}'.format(i, ''.ljust(10, '-'), c))

        print()

        if default is None:
            choice = input('Selection: ')
        else:
            choice = input(f'Selection [{choices.index(default)}]: ')

    if default is not None and choice == '':
        return default
    else:
        return choices[int(choice)]




def get_input(prompt):
    while True:
        response = input(prompt)

        if yes_no(f'Confirm: "{response}" [Y]/N '):
            return response


def pull_source(dst_dir: tempfile.TemporaryDirectory, args: argparse.Namespace):
    ASF = 'ASF subversion (dist.apache.org)'
    GIT = 'GitHub'
    LFS = 'Local Filesystem'

    source_location = choice_prompt(
        'Where is the source you\'re building from stored?',
        [ASF, GIT, LFS],
        ASF
    )

    if source_location == ASF:
        ...
    elif source_location == GIT:
        ...
    else:
        ...


def main():
    parser = argparse.ArgumentParser(
        epilog="With the exception of the --skip-nexus, --skip-ingester, and --tag-suffix options, the user will be "
               "prompted to set options at runtime."
    )

    parser.add_argument(
        '--nexus-version',
        dest='v_nexus',
        help='Version of Nexus to download and build',
        metavar='VERSION'
    )

    parser.add_argument(
        '--ingester-version',
        dest='v_ingester',
        help='Version of Ingester to download and build',
        metavar='VERSION'
    )

    parser.add_argument(
        '--docker-registry',
        dest='registry',
        help='Docker registry to tag images with. Important if you want to push the images.'
    )

    cache = parser.add_mutually_exclusive_group(required=False)

    cache.add_argument(
        '--no-cache',
        dest='cache',
        action='store_false',
        help='Don\'t use build cache'
    )

    cache.add_argument(
        '--cache',
        dest='cache',
        action='store_true',
        help='Use build cache'
    )

    push = parser.add_mutually_exclusive_group(required=False)

    push.add_argument(
        '--push',
        dest='push',
        action='store_true',
        help='Push images after building'
    )

    push.add_argument(
        '--no-push',
        dest='push',
        action='store_false',
        help='Don\'t push images after building'
    )

    parser.add_argument(
        '--dl-url',
        dest='url',
        help='Root url to download tarballs from. Eg: https://dist.apache.org/repos/dist/dev/incubator/sdap/apache-sdap-1.2.0-rc3/',
    )

    parser.add_argument(
        '--skip-nexus',
        dest='skip_nexus',
        action='store_true',
        help='Don\'t build Nexus webapp, Solr cloud & Solr cloud init images'
    )

    parser.add_argument(
        '--skip-ingester',
        dest='skip_ingester',
        action='store_true',
        help='Don\'t build Collection Manager & Granule Ingester images'
    )

    parser.add_argument(
        '--tag-suffix',
        dest='tag_suffix',
        help='Suffix to append to image tags. For example, if you want to build images with tags ending with "-rc0" '
             'etc',
        metavar='STRING'
    )

    parser.set_defaults(cache=None, push=None)

    args = parser.parse_args()

    v_nexus, v_ingester, registry, cache, push = args.v_nexus, args.v_ingester, args.registry, args.cache, args.push

    # TODO: Support pulling from any ASF release/stage location, OR git repo OR local FS. And make it more interactive

    root_url = args.url

    if root_url is None:
        root_url = get_input('Enter URL to download release from\n'
                             '(Eg: https://dist.apache.org/repos/dist/dev/incubator/sdap/apache-sdap-1.2.0-rc3/) : ')

    if v_nexus is None and not args.skip_nexus:
        v_nexus = get_input('Enter NEXUS version to build: ')

    if v_ingester is None and not args.skip_ingester:
        v_ingester = get_input('Enter Ingester version to build: ')

    if registry is None:
        registry = get_input('Enter Docker image registry: ')

    if cache is None:
        cache = yes_no('Use Docker build cache? [Y]/N: ')

    if push is None:
        push = yes_no('Push built images? [Y]/N: ')

    print('Downloading release tarballs...')

    extract_dir = tempfile.TemporaryDirectory()

    nexus_tarball = os.path.join(extract_dir.name, NEXUS_TARBALL.format(v_nexus))
    ingester_tarball = os.path.join(extract_dir.name, INGESTER_TARBALL.format(v_ingester))

    if not args.skip_nexus:
        response = requests.get(urljoin(root_url, NEXUS_TARBALL.format(v_nexus)))
        response.raise_for_status()

        with open(nexus_tarball, 'wb') as fp:
            fp.write(response.content)
            fp.flush()

    if not args.skip_ingester:
        response = requests.get(urljoin(root_url, INGESTER_TARBALL.format(v_ingester)))
        response.raise_for_status()

        with open(ingester_tarball, 'wb') as fp:
            fp.write(response.content)
            fp.flush()

    print('Extracting release source files...')

    if not args.skip_nexus:
        run(
            ['tar', 'xvf', nexus_tarball, '-C', extract_dir.name],
            suppress_output=True
        )
        shutil.move(
            os.path.join(extract_dir.name, 'Apache-SDAP', NEXUS_DIR.format(v_nexus)),
            os.path.join(extract_dir.name, 'nexus')
        )

    if not args.skip_ingester:
        run(
            ['tar', 'xvf', ingester_tarball, '-C', extract_dir.name],
            suppress_output=True
        )
        shutil.move(
            os.path.join(extract_dir.name, 'Apache-SDAP', INGESTER_DIR.format(v_ingester)),
            os.path.join(extract_dir.name, 'ingester')
        )

    os.environ['DOCKER_DEFAULT_PLATFORM'] = 'linux/amd64'

    built_images = []

    def tag(s):
        if args.tag_suffix is not None:
            return f'{s}-{args.tag_suffix}'
        else:
            return s

    if not args.skip_ingester:
        print('Building ingester images...')

        cm_tag = tag(f'{registry}/sdap-collection-manager:{v_ingester}')

        run(build_cmd(
            cm_tag,
            os.path.join(extract_dir.name, 'ingester'),
            dockerfile='collection_manager/docker/Dockerfile',
            cache=cache
        ))

        built_images.append(cm_tag)

        gi_tag = tag(f'{registry}/sdap-granule-ingester:{v_ingester}')

        run(build_cmd(
            gi_tag,
            os.path.join(extract_dir.name, 'ingester'),
            dockerfile='granule_ingester/docker/Dockerfile',
            cache=cache
        ))

        built_images.append(gi_tag)

    if not args.skip_nexus:
        solr_tag = tag(f'{registry}/sdap-solr-cloud:{v_nexus}')

        run(build_cmd(
            solr_tag,
            os.path.join(extract_dir.name, 'nexus/docker/solr'),
            cache=cache
        ))

        built_images.append(solr_tag)

        solr_init_tag = tag(f'{registry}/sdap-solr-cloud-init:{v_nexus}')

        run(build_cmd(
            solr_init_tag,
            os.path.join(extract_dir.name, 'nexus/docker/solr'),
            dockerfile='cloud-init/Dockerfile',
            cache=cache
        ))

        built_images.append(solr_init_tag)

        webapp_tag = tag(f'{registry}/sdap-nexus-webapp:{v_nexus}')

        run(build_cmd(
            webapp_tag,
            os.path.join(extract_dir.name, 'nexus'),
            dockerfile='docker/nexus-webapp/Dockerfile',
            cache=cache
        ))

        built_images.append(webapp_tag)

    print('Image builds completed')

    if push:
        for image in built_images:
            run(
                [DOCKER, 'push', image]
            )

    print('done')


if __name__ == '__main__':
    main()
