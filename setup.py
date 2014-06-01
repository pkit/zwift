#!/usr/bin/python

from setuptools import setup, find_packages

name = 'zwift'
version = '0.1'

setup(
    name=name,
    version=version,
    description='zwift',
    license='',
    author='',
    author_email='',
    url='',
    packages=find_packages(exclude=['test', 'bin']),
    test_suite='nose.collector',
    classifiers=[],
    install_requires=[],
    scripts=[],
    entry_points={
        'paste.app_factory': [
            'container=zwift.container.server:app_factory'
        ],
    },
)

