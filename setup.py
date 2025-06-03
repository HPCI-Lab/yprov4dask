#!/usr/bin/env python

from setuptools import setup

setup(
  name='yprov4dask',
  version='0.1',
  description='Plugin for the Dask scheduler that enables provance tracking',
  author='Leonardo De Faveri',
  author_email='',
  url='https://github.com/HPCI-Lab/yprov4dask',
  package_dir={'': 'src'},
  packages=['prov_tracking'],
  install_requires = [
    'distributed', # 2025.5.1
    'dask', # 2025.5.1
    'prov', # 2.0.1
  ]
)