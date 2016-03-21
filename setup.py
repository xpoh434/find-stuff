#!/usr/bin/env python
from distutils.core import setup

setup(name='find-stuff',
      version='1.0',
      description='find-stuff',
      url='https://github.com/xpoh434/find-stuff/',
      packages=['find_stuff'],
      install_requires=['whoosh','pdfminer']
     )
