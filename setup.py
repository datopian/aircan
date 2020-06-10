from setuptools import setup

setup(
    name='AirCan',
    packages=['aircan'],
    version=open('VERSION').read(),
    description='',
    author='Datopian',
    author_email='info@datopian.com',
    install_requires=[
        'apache-airflow',
    ],
    package_data={}
)
