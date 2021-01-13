#!/usr/bin/env python

from setuptools import setup

setup(name='py-evolve',
      version='1.3',
      description='Python library to submit workflows in evolve infrastructure',
      url='https://sunlight.io/',
      author='Sunlight.io',
      license='GPL',
      packages=['py_evolve'],
      install_requires=['requests>=2.23'],
      classifiers=['Development Status :: 4 - Beta',
                   'Environment :: Console',
                   'Programming Language :: Python :: 2.7',
                   'Programming Language :: Python :: 3.6',
                   'Operating System :: OS Independent',
                   'Topic :: Software Development :: Libraries :: Python Modules'
                   'License :: OSI Approved :: Apache Software License'])
