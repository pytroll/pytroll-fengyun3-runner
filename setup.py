#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2013, 2014, 2015, 2016 Martin Raspaud

# Author(s):

#   Martin Raspaud <martin.raspaud@smhi.se>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Setup for aapp-runner.
"""
from setuptools import setup
import imp

version = imp.load_source('modis_runner.version', 'modis_runner/version.py')

setup(name="modis_runner",
      version=version.__version__,
      description='Pytroll runner for AAPP',
      author='Adam Dybroe',
      author_email='adam.dybroe@smhi.se',
      classifiers=["Development Status :: 3 - Alpha",
                   "Intended Audience :: Science/Research",
                   "License :: OSI Approved :: GNU General Public License v3 " +
                   "or later (GPLv3+)",
                   "Operating System :: OS Independent",
                   "Programming Language :: Python",
                   "Topic :: Scientific/Engineering"],
      url="https://github.com/pytroll/pytroll-aapp-runner",
      packages=['modis_runner', ],
      scripts=['bin/modis_dr_runner.py', 'bin/seadas_modis_runner.py', ],
      data_files=[],
      zip_safe=False,
      install_requires=['posttroll', ],
      # test_requires=['mock'],
      # test_suite='modis_runner.tests.suite',
      )
