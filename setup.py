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
import os
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
KAFKA_EXPECTED_HASH_SHA256 = '93b6f926b10b3ba826266272e3bd9d0fe8b33046da9a2688c58d403eb0a43430'  # noqa
KAFKA_TAR = 'kafka.tgz'
KAFKA_TAR_ROOTDIR = 'kafka_2.11-1.1.1'
KAFKA_DIR = 'swh/journal/tests/kafka'


def set_up_kafka():
    """Download Kafka from an official mirror and untar it.  The tarball
    is checked for the right checksum.

    The data are not cleaned up to allow caching.  Call specific
    `setup.py clean` to clean up the test artefacts.

    """
    if not os.path.exists(KAFKA_TAR):
        print('*swh-journal-setup* Downloading Kafka', file=sys.stderr)
        urllib.request.urlretrieve(KAFKA_URL, KAFKA_TAR)
        import hashlib
        h = hashlib.sha256()
        with open(KAFKA_TAR, 'rb') as f:
            for chunk in f:
                h.update(chunk)
        hash_result = h.hexdigest()
        if hash_result != KAFKA_EXPECTED_HASH_SHA256:
            raise ValueError(
                "Mismatch tarball %s hash checksum: expected %s, got %s" % (
                    KAFKA_TAR, KAFKA_EXPECTED_HASH_SHA256, hash_result, ))

    if not os.path.exists(KAFKA_DIR):
        print('*swh-journal-setup* Unpacking Kafka', file=sys.stderr)
        with tarfile.open(KAFKA_TAR, 'r') as f:
            f.extractall()

        print('*swh-journal-setup* Renaming:', KAFKA_TAR_ROOTDIR, '→',
              KAFKA_DIR, file=sys.stderr)
        Path(KAFKA_TAR_ROOTDIR).rename(KAFKA_DIR)


class InstallExtraDevDependencies(develop):
    """Install development dependencies and download/setup Kafka.

    """
    def run(self):
        """Set up the local dev environment fully.

        """
        print('*swh-journal-setup* Installing dev dependencies',
              file=sys.stderr)
        super().run()
        subprocess.check_call(['pip', 'install', '-U', 'pip'])
        subprocess.check_call(['pip', 'install', '.[testing]'])

        print('*swh-journal-setup* Setting up Kafka', file=sys.stderr)
        set_up_kafka()
        print('*swh-journal-setup* Setting up Kafka done')


class CleanupExtraDevDependencies(develop):
    def run(self):
        """Clean whatever `set_up_kafka` may create.

        """
        print('*swh-journal-setup* Cleaning up: %s, %s, %s' % (
            KAFKA_DIR, KAFKA_TAR_ROOTDIR, KAFKA_TAR))
        shutil.rmtree(KAFKA_DIR, ignore_errors=True)
        shutil.rmtree(KAFKA_TAR_ROOTDIR, ignore_errors=True)
        with contextlib.suppress(FileNotFoundError):
            Path(KAFKA_TAR).unlink()
        print('*swh-journal-setup* Cleaning up done')


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
        'develop': InstallExtraDevDependencies,
        'clean': CleanupExtraDevDependencies,
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
