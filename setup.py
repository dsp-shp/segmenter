#!/usr/bin/env python
from json import loads
from setuptools import setup, find_packages

setup(
    **loads(open('info.json').read()),
    packages=find_packages(),
    include_package_data=True,
    long_description=open('README.md').read(),
    install_requires=open('requirements.txt').read().splitlines()
)