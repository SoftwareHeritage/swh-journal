#!/usr/bin/env python3
# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# Regarding the kafka installation development code (test)
# Copyright 2018 Infectious Media Ltd and pytest-kafka contributors
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
# BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import contextlib
import tarfile
import shutil
import subprocess
import sys
import urllib.request

from pathlib import Path
from setuptools import setup, find_packages
from setuptools.command.develop import develop  # type: ignore


from os import path
from io import open

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


def parse_requirements(name=None):
    if name:
        reqf = 'requirements-%s.txt' % name
    else:
        reqf = 'requirements.txt'

    requirements = []
    if not path.exists(reqf):
        return requirements

    with open(reqf) as f:
        for line in f.readlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            requirements.append(line)
    return requirements


# Dependencies for the test, copied from pytest-kafka's setup.py [1]
# [1] https://gitlab.com/karolinepauls/pytest-kafka/blob/master/setup.py
KAFKA_URL = (
    'https://www.mirrorservice.org/sites/ftp.apache.org/kafka/1.1.1/'
    'kafka_2.11-1.1.1.tgz')
KAFKA_TAR = 'kafka.tgz'
KAFKA_TAR_ROOTDIR = 'kafka_2.11-1.1.1'
KAFKA_DIR = 'kafka'


def set_up_kafka():
    """Clean, download Kafka from an official mirror and untar it.

    """
    clean_kafka()

    print('* Downloading Kafka', file=sys.stderr)
    urllib.request.urlretrieve(KAFKA_URL, KAFKA_TAR)

    print('* Unpacking Kafka', file=sys.stderr)
    with tarfile.open(KAFKA_TAR, 'r') as f:
        f.extractall()

    print('* Renaming:', KAFKA_TAR_ROOTDIR, '→', KAFKA_DIR, file=sys.stderr)
    Path(KAFKA_TAR_ROOTDIR).rename(KAFKA_DIR)
    Path(KAFKA_TAR).unlink()


def clean_kafka():
    """Clean whatever `set_up_kafka` may create.

    """
    shutil.rmtree(KAFKA_DIR, ignore_errors=True)
    shutil.rmtree(KAFKA_TAR_ROOTDIR, ignore_errors=True)
    with contextlib.suppress(FileNotFoundError):
        Path(KAFKA_TAR).unlink()


class InstallDevDependencies(develop):
    """Install development dependencies and download/setup Kafka.

    """
    def run(self):
        """Set up the local dev environment fully.

        """
        super().run()
        print('* Installing dev dependencies', file=sys.stderr)
        subprocess.check_call(['pip', 'install', '-U', 'pip'])
        subprocess.check_call(['pip', 'install', '.[testing]'])

        print('* Setting up Kafka', file=sys.stderr)
        set_up_kafka()


setup(
    name='swh.journal',
    description='Software Heritage Journal utilities',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Software Heritage developers',
    author_email='swh-devel@inria.fr',
    url='https://forge.softwareheritage.org/diffusion/DJNL/',
    packages=find_packages(),
    scripts=[],
    entry_points='''
        [console_scripts]
        swh-journal=swh.journal.cli:main
    ''',
    install_requires=parse_requirements() + parse_requirements('swh'),
    setup_requires=['vcversioner'],
    extras_require={
        'testing': parse_requirements('test')
    },
    vcversioner={},
    include_package_data=True,
    cmdclass={
        'develop': InstallDevDependencies,
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
    ],
    project_urls={
        'Bug Reports': 'https://forge.softwareheritage.org/maniphest',
        'Funding': 'https://www.softwareheritage.org/donate',
        'Source': 'https://forge.softwareheritage.org/source/swh-journal',
    },
)
