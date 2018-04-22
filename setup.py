#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='pg_dump_filtered',
    packages=find_packages(),
    author="Benjamin BERNARD",
    author_email="benjamin.bernard@openpathview.fr",
    description="Dump a postgres database partially using filters on tables and extract all related necessary datas.",
    long_description=open('README.md').read(),
    dependency_links=[],
    install_requires=[],
    # Active la prise en compte du fichier MANIFEST.in
    include_package_data=True,
    url='https://github.com/OpenPathView/OPV_importData',
    entry_points={
        'console_scripts': [
            'pg-dump-filtered = pg_dump_filtered.__main__:main'
        ],
    }
)
