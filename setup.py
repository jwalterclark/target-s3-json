#!/usr/bin/env python

from setuptools import setup

setup(name='target-s3-json',
      version='0.0.01',
      description='Singer.io target for writing JSON files and upload to S3 - PipelineWise compatible',
      author='wclark',
      url='https://github.com/jwalterclark/target-s3-json',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['target_s3_json'],
      install_requires=[
          'jsonschema==2.6.0',
          'singer-python==5.0.4',
          'inflection==0.3.1',
          'boto3==1.9.57',
          'backoff==1.3.2'
      ],
      entry_points='''
          [console_scripts]
          target-s3-json=target_s3_json:main
      ''',
      packages=["target_s3_json"],
      package_data={},
      include_package_data=True,
      )
