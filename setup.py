from setuptools import setup

setup(
    name='stats',
    url='https://github.com/PatrickChodowski/stats',
    author='Patrick Chodowski',
    author_email='chodowski.patrick@gmail.com',
    packages=['stats'],
    install_requires=['matplotlib', 'google-cloud-bigquery', 'pandas', 'numpy'],
    # *strongly* suggested for sharing
    version='0.6.0',
    license='GPLv3',
    description='Module for plotting data straight from GBQ',
    long_description=open('readme.md').read(),
)
