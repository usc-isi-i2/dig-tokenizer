try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'name': 'digTokenizer',
    'description': 'digTokenizer',
    'author': 'Rajagopal Bojanapalli',
    'url': 'https://github.com/usc-isi-i2/dig-tokenizer',
    'download_url': 'https://github.com/usc-isi-i2/dig-tokenizer',
    'author_email': 'rajagopal067@gmail.com',
    'version': '0.3',
    'install_requires': ['nose2',
                         'digSparkUtil'],
    # dependency_links=['http://github.com/user/repo/tarball/master#egg=package-1.0']
    # these are the subdirs of the current directory that we care about
    'packages': ['digTokenizer'],
    'scripts': [],
}

setup(**config)
