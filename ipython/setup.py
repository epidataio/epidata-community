#
# Copyright (c) 2015-2022 EpiData, Inc.
#

from setuptools import setup

setup(
    name='epidata',
    version='1.0-SNAPSHOT',
    description='Epidata query and analytics library',
    author='Epidata',
    url='https://epidata.io',
    packages=['epidata', 'epidata._private'],
    long_description="""\
      Epidata query and analytics library
      """,
    classifiers=[
        "Programming Language :: Python",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers"
    ],
    keywords='epidata',
    install_requires=[
        'setuptools'
    ],
)
