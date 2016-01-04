# coding: utf-8

from setuptools import setup, find_packages


setup(
    name='libra',
    version='0.0.10',
    description='Load balancing algorithm',
    long_description="Load balancing",
    author='xuweizheng; ctycheer',
    author_email='xuweizheng@sohu-inc.com',
    url='http://m.sohu.com/',
    packages=find_packages(exclude=["*.pyc"]),
    install_requires=[
        'tornado>=2.4.1',
    ],
    classifiers=[
        "Topic: Load balancing algorithm",
        "Operating System :: Ubuntu",
        "Programming Language :: Python",
    ],
)
