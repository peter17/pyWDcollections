#!/usr/bin/env python3
# -*- coding: utf8 -*-

'''
Copyright 2016 Peter Potrowl <peter.potrowl@gmail.com>

This file is part of pyWDcollections.

pyWDcollections is free software: you can redistribute it and/or modify it
under the terms of the GNU Lesser General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

pyWDcollections is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with Pijnu.  If not, see <http://www.gnu.org/licenses/>.
'''


from setuptools import setup

long_description = """\
'pyWDcollections' is
        * a framework to build Wikidata bots,
        * a tool to collect and update Wikidata items.
"""

setup(name="pyWDcollections",
      version="0.0.1",
      author="Peter Potrowl",
      author_email="peter.potrowl@gmail.com",
      maintainer="Peter Potrowl",
      maintainer_email="peter.potrowl@gmail.com",
      url="https://github.com/peter17/pyWDcollections",
      license="LGPL v3",
      platforms=["Any"],
      packages=["pywdcollections"],
      scripts=[],
      install_requires=['pywikibot'],
      description="tool to collect and update Wikidata items",
      long_description=long_description,
      classifiers=[
          'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
          'Development Status :: 4 - Beta',
          'Topic :: Software Development :: Libraries :: Python Modules',
          'Intended Audience :: Developers',
          'Programming Language :: Python',
          ]
      )
