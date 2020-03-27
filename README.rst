SQLBucket
=========

.. image:: https://travis-ci.org/socialpoint-labs/sqlbucket.svg?branch=master
    :target: https://travis-ci.org/socialpoint-labs/sqlbucket


SQLBucket is a lightweight framework to help write, orchestrate and validate 
SQL data pipelines. It gives the possibility to set variables and introduces
some control flow using the fantastic Jinja2 library. It also implements a 
very simplistic unit and integration test framework where you can validate the
results of your ETL in the form of SQL checks. With SQLBucket, you can apply 
TDD principles when writing data pipelines.

It can work as a stand alone service, or be part of your workflow
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

To start working, you need to instantiate your SQLBucket core object with the
`project_folder` parameter. That folder will contain all your SQL ETL. The
python file where you create your SQLBucket object is also a good place to
instantiate your command line interface, as shown below.

.. code-block:: python

    # my_sqlbucket.py
    from sqlbucket import SQLBucket


    bucket = SQLBucket(projects_folder='projects')


    if __name__ == '__main__':
        bucket.cli()


The following command will create your first project in your `projects folder`.

.. code-block:: bash

    python my_sqlbucket.py create-project -n my_first_project

For more info on CLI, please refer to `its documentations`_.

.. _its documentations: https://github.com/socialpoint-labs/sqlbucket/blob/master/documentation/cli.rst


Your `projects` should now look like the structure below:

.. code-block:: bash

    projects/
        |-- my_first_project/
            |-- config.yaml
            |-- queries/
                |-- query_one.sql
                |-- query_two.sql
            |-- integrity/
                |-- integrity_one.sql


An SQLBucket project is made of 3 components:
 * Configuration
 * ETL queries
 * Integrity queries

**Configuration**

The `config.yaml` is the core of your project. This is where you can define
variables at project level, and configure the order your sql queries must be
executed. For a better explanations on how to configure variables you can refer
to the `usage documentation`_, and also the `variables documentation`_ which
also describes environment and connections variables.

.. _usage documentation: https://github.com/socialpoint-labs/sqlbucket/blob/master/documentation/usage.rst
.. _variables documentation: https://github.com/socialpoint-labs/sqlbucket/blob/master/documentation/variables.rst


**ETL queries**

The `queries` folder simply contain your SQL queries. You can organize them in
the folder structure of your choice. As long as they are in the `queries`
folder, SQLBucket will find them and execute them when configured to do so.
See the documentation on `how to write SQL with SQLBucket`_.

.. _how to write SQL with SQLBucket: https://github.com/socialpoint-labs/sqlbucket/blob/master/documentation/usage.rst


**Integrity queries**

The `integrity` folder simply contain SQL queries to help you validate your
ETL. You can organize them in the folder structure of your choice. The only
convention is to return the result of your integrity (True/False) in a field
named `passed`. The main idea is that integrity is done by SQL itself.
Check `documentation on integrity`_ for a more detailed explanation on testing
the integrity of your ETL. We also have a set of `common macros`_ that can be
helpful to start with.

.. _documentation on integrity: https://github.com/socialpoint-labs/sqlbucket/blob/master/documentation/integrity.rst
.. _common macros: https://github.com/socialpoint-labs/sqlbucket/blob/master/documentation/integrity.rst


See below a full example that will actually first run your ETL, and then run
your integrity checks for a given database configuration.


.. code-block:: python

    from sqlbucket import SQLBucket

    connections = {
        'db_demo': 'postgresql://user:password@host:5439/database'
    }

    bucket = SQLBucket(connections=connections)
    project = bucket.load_project(
        project_name='my_first_project',
        connection_name='db_demo',
        variables={'foo': 1}
    )

    # to run ETL
    project.run()

    # to run integrity
    project.run_integrity()


We recommend setting your connection urls as environment variables for security
purposes.

Template project
----------------

To get you up to speed, you can create a fork of the `SQLBucket template project`_
and start building SQL data pipelines within minutes.

.. _SQLBucket template project: https://github.com/philippe2803/sqlbucket-template


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

