# coding: utf-8

import re
from setuptools import setup, find_packages

# Get the version
version_regex = r'__version__ = ["\']([^"\']*)["\']'
with open('libra/__init__.py', 'r') as f:
    text = f.read()
    match = re.search(version_regex, text)

    if match:
        version = match.group(1)
    else:
        raise RuntimeError("No version number found!")

setup(
    name='libra',
    version=version,
    author='xuweizheng; ctycheer',
    author_email='xuweizheng@sohu-inc.com',
    url='http://m.sohu.com/',
    packages=find_packages(exclude=["*.pyc"]),
    package_data={
        'libra': ['conf/*.yaml']
    },
    install_requires=[
        'python-etcd==0.4.4.dev0',
        'urllib3==1.14.dev1',
        'pyyaml',
        'enum34>=1.1.6',
    ],
    extras_require={
        'http2': ['http2>=0.2.4'],
        'zmq': ['pyzmq==15.2.0'],
        'config': ['deepdiff=2.5.1'],
        'consistent_statsd': ['hash_ring==1.3.1', 'statsd==3.2.1'],
    },
)
