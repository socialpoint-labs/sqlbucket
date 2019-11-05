SQLBucket
=========

.. image:: https://travis-ci.org/socialpoint-labs/sqlbucket.svg?branch=master
    :target: https://travis-ci.org/socialpoint-labs/sqlbucket


SQLBucket was built to help write, orchestrate and validate SQL ETL. It
gives the possibility to set variables and introduces some control flow
like if/else and for loops, to make your queries dynamic when writing them. It
also implements a very simplistic integration testing framework to validate the
results of your ETL pipelines in the form of SQL checks.

Lightweight, it can work as a stand alone service, or be part of your workflow
manager environment (Airflow, Luigi, ..).


Installing
----------

Install and update using `pip`_:

.. code-block:: text

    pip install -U sqlbucket

SQLBucket works only for Python 3.6 and 3.7, and probably 3.8 although
not tested yet.


A Simple Example
----------------


To start working with SQLBucket, you need to have a 'projects' folder that will
contain all your SQL ETL. SQLBucket, essentially works with the following folder
structure as a root folder, where you can have as many projects as you want.


.. code-block:: bash

    projects/
        |-- project1/
        |-- project2/
        |-- project3/
            ...


Inside a project, it must contain a folder called `queries` that will contains
your SQL files to be ran, and a config.yaml that will let you set the order in
which those queries must be processed. SQLBucket leverages on the amazing
Jinja2 templating library to give you the possibility to set variables in your
SQL as well as giving you pure execution flows like for loops. To see in more
depth what can be done, see the documentation on `how to write SQL in SQLBucket`_.

.. _how to write SQL in SQLBucket: https://github.com/socialpoint-labs/sqlbucket/blob/master/documentation/usage.rst


In practice, this is how a project folder would look.

.. code-block:: bash

    projects/
        |-- my_super_project/
            |-- config.yaml
            |-- queries/
                |-- my_super_insert_query.sql
                |-- some_other_query.sql
            |-- integrity/
                |-- test1.sql
                |-- test2.sql

The `integrity` folder gives you the possibility to write some checks in SQL,
that will ran at the end of your ETL to validate your data. Check `documentation
on integrity`_ for a more detailed explanation on testing the integrity of your
ETL.

.. _documentation on integrity: https://github.com/socialpoint-labs/sqlbucket/blob/master/documentation/integrity.rst


This is how you would launch a project:

.. code-block:: python

    from sqlbucket import SQLBucket

    connections = {
    'db_test': 'postgresql://user:password@host:5439/database'
    }

    bucket = SQLBucket(connections=connections)
    project = bucket.load_project(
        project_name='fat_etl',
        connection_name='db_test',
        variables={'foo': 1}
    )

    project.run()
    project.run_integrity()


This would trigger logs as below.

.. image:: documentation/images/terminal.gif
    :align: center


You can also launch a project using the `sqlbucket command line interface`_.

.. _sqlbucket command line interface: https://github.com/socialpoint-labs/sqlbucket/blob/master/documentation/cli.rst



Contributing
------------

For guidance on how to make a contribution to SQLBucket, see the `contributing guidelines`_.

.. _contributing guidelines: https://github.com/socialpoint-labs/sqlbucket/blob/master/CONTRIBUTING.rst


Links
-----

* License: `MIT <https://github.com/socialpoint-labs/sqlbucket/blob/master/LICENSE>`_
* Releases: https://pypi.org/project/sqlbucket/
* Code: https://github.com/socialpoint-labs/sqlbucket
* Issue tracker: https://github.com/socialpoint-labs/sqlbucket/issues


.. _pip: https://pip.pypa.io/en/stable/quickstart/

