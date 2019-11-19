import io
import re
from setuptools import setup


with io.open('sqlbucket/__init__.py', 'rt', encoding='utf8') as f:
    version = re.search(
        r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
        f.read(),
        re.MULTILINE
    ).group(1)


setup(
    name='sqlbucket',
    packages=[
        'sqlbucket',
        'sqlbucket.macros',
        'sqlbucket.template',
        'sqlbucket.template.queries',
        'sqlbucket.template.integrity'
    ],
    include_package_data=True,
    description='SQLBucket - Write your SQL ETL flow and ETL integrity tool.',
    long_description=open('README.rst', 'r').read(),
    version=version,
    author='Philippe Oger',
    author_email='phil.oger@gmail.com',
    url='https://github.com/socialpoint-labs/sqlbucket',
    keywords=['sql', 'etl', 'data-integrity'],
    install_requires=[
        'click>=6.0',
        'jinja2==2.10.1',
        'markupsafe==1.1.1',
        'pyyaml>=5.0',
        'sqlalchemy~=1.3',
        'tabulate>=0.7.5'
    ],
    extras_require={
        'dev': [
            'pytest>=4',
            'coverage'
        ]
    },
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: End Users/Desktop",
        "Intended Audience :: Science/Research",
        "Topic :: Database :: Front-Ends",
        "Topic :: Software Development :: Libraries :: Python Modules"
        ],
    license='MIT'
)
