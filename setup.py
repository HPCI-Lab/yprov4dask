#!/usr/bin/env python

from setuptools import setup

setup(
  name='yprov4dask',
  version='0.1',
  description='Plugin for the Dask scheduler that enables provance tracking',
  author='Leonardo De Faveri',
  author_email='leonardo.defaveri01@gmail.com',
  url='https://github.com/HPCI-Lab/yprov4dask',
  packages=['prov_tracking'],
  install_requires = [
    'distributed',
    'dask',
    'prov',
  ]
)