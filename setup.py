import sys
import unittest
from setuptools import setup, find_packages

version = '0.1.1'
if not sys.version_info[0] == 3 and not sys.version_info[1] >= 5:
    sys.exit("Sorry, Python 3.5 or greater required.")

print (find_packages())

def get_test_suite():
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('lib/komlogd', pattern='test_*.py')
    return test_suite

setup(
    name='komlogd',
    version=version,
    description='Komlog agent',
    author='komlog Team',
    author_email='hello@komlog.io',
    url='https://github.com/komlog-io/komlogd',
    license='Apache Software License',
    install_requires=['pyyaml', 'setuptools', 'cryptography', 'aiohttp>=1.0', 'pandas'],
    package_dir={ '': 'lib' },
    packages = find_packages('lib'),
    test_suite = 'setup.get_test_suite',
    scripts=[
       'bin/komlogd',
    ],
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
