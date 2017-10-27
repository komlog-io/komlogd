import os
import sys
import unittest
from setuptools import setup, find_packages

exec(open('lib/komlogd/version.py').read())

if not sys.version_info[0] == 3 and not sys.version_info[1] >= 5:
    sys.exit("Sorry, Python 3.5 or greater required.")


setup(
    name='komlogd',
    version=__version__,
    description='Komlog agent',
    long_description=open(os.path.join(os.path.dirname(__file__), 'docs/.pypi_readme.rst')).read(),
    author='komlog Team',
    author_email='hello@komlog.io',
    url='https://github.com/komlog-io/komlogd',
    license='Apache Software License',
    install_requires=['pyyaml', 'setuptools', 'cryptography>=2.0', 'aiohttp>=2.0', 'pandas>=0.18.1'],
    package_dir={ '': 'lib' },
    packages = find_packages('lib'),
    test_suite = 'komlogd',
    entry_points = {
        'console_scripts': [
            'komlogd = komlogd.main:main'
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Build Tools',
        'Topic :: System :: Monitoring',
        'Topic :: System :: Systems Administration',
        'Topic :: Utilities',
    ]
)
