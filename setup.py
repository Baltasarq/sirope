# Sirope (c) Baltasar 2022 MIT License <baltasarq@gmail.com>


import os
from distutils.core import setup


setup(
    name = 'sirope',
    packages = ['sirope'],
    version = '0.1.5',
    license='MIT',
    description = 'Simple Interface for Redis of Object Persistence',
    author = 'Baltasar',
    author_email = 'baltasarq@gmail.com',
    url = 'https://github.com/baltasarq/sirope/',
    download_url = 'https://github.com/baltasarq/sirope/releases/latest',
    keywords = ['REDIS', 'JSON', 'ORM'],
    install_requires=['redis'],
    classifiers=[
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'Topic :: Database',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3',
    ],
)
