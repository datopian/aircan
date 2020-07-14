from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='aircan',
    packages=find_packages(exclude=['examples', 'docs', 'tests*']),
    version=open('VERSION').read(),
    description='',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Datopian',
    author_email='info@datopian.com',
    install_requires=[
        'apache-airflow',
        'psycopg2',
        'python-dotenv',
        'requests',
        'sqlalchemy'
    ],
    package_data={},
    keywords='aircan, airflow, aircan-lib',
    python_requires='<=3.7.8',
)
