#!/usr/bin/env python

try:
        from setuptools import setup, find_packages
except ImportError:
        from distutils.core import setup, find_packages

setup(
    name = 'release_tool',
    version = '0.1',
    description = '',
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    install_requires = ['Click', 'PyYAML', 'xmltodict'],
    entry_points={
        'console_scripts': ['release_tool=release_tool.cli:main'],
    },
)
