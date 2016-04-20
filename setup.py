try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import os
import io
import re

def read(*names, **kwargs):
    with io.open(
        os.path.join(os.path.dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8")
    ) as fp:
        return fp.read()

def find_version(*file_paths):
    version_file = read(*file_paths)
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

config = {
    'name': 'digTokenizer',
    'description': 'digTokenizer',
    'author': 'Rajagopal Bojanapalli',
    'url': 'https://github.com/usc-isi-i2/dig-tokenizer',
    'download_url': 'https://github.com/usc-isi-i2/dig-tokenizer',
    'author_email': 'bojanapa@usc.edu',
    'install_requires': [
                         'digSparkUtil'
                         ],
    # dependency_links=['http://github.com/user/repo/tarball/master#egg=package-1.0']
    # these are the (sub)modules of the current directory that we care about
    'packages': ['digTokenizer', 'digTokenizer.inputParser', 'digTokenizer.ngram'],
    'scripts': [],
    'version': find_version("digTokenizer", "__init__.py")
}

setup(**config)
