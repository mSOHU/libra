# coding: utf-8

from setuptools import setup, find_packages


setup(
    name='libra',
    version='0.1.3',
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
        'pyyaml',
    ],
)
