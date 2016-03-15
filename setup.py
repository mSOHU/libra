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
        'libra': ['conf/config.yaml']
    },
    install_requires=[
        'python-etcd==0.4.3',
        'urllib3==1.14.dev1',
        'http2==0.2.2',
        'pyyaml',
        'pyzmq==15.2.0',
    ],
)
