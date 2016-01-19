# coding: utf-8

from setuptools import setup, find_packages


setup(
    name='libra',
    version='0.1.0',
    author='xuweizheng; ctycheer',
    author_email='xuweizheng@sohu-inc.com',
    url='http://m.sohu.com/',
    packages=find_packages(exclude=["*.pyc"]),
    install_requires=[
        'python-etcd==0.4.3'
        'pyyaml'
    ],
)
