#coding: utf-8
from setuptools import setup, find_packages

PACKAGE = "libra"
NAME = "libra"
DESCRIPTION = "Load balancing algorithm"
AUTHOR = "xuweizheng; ctycheer"
AUTHOR_EMAIL = "xuweizheng@sohu-inc.com"
URL = "http://m.sohu.com/"


setup(
    name=NAME,
    version='0.0.4',
    description=DESCRIPTION,
    long_description="Load balancing",
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    packages=find_packages(exclude=["*.pyc"]),
    install_requires=[

    ],
    classifiers=[
        "Topic: Load balancing algorithm",
        "Operating System :: Ubuntu",
        "Programming Language :: Python",

    ],

    # entry_points={
    # },

)
