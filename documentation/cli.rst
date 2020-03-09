Command Line Interface for SQLBucket
====================================


To access the command line interface for a specific SQLBucket you need to
create it as follows on the file of your choice:


.. code-block:: python

    from sqlbucket import SQLBucket

    sqlbucket = SQLBucket()

    if __name__ == '__main__':
        sqlbucket.cli()


Let's assume you put that python code on a file called cli.py, you could then
run the CLI that way:


.. code-block:: bash

    python cli.py run-job -n my_etl -b dbname

This will run the project called `my_etl` for the database you called `dbname`
in your connection configuration.


The CLI currently has 3 commands:
    * create-project: build a project in your project folder with the right folder convention.
    * run-job: run a project ETL
    * run-integrity: run data integrity checks queries for a given project.



Command: create-project
-----------------------

+-----------------+--------------+----------------------+
| **parameter**   | **shortcut** | **description**      |
+-----------------+--------------+----------------------+
| ``--name``      | ``-n``       |  Name of new project |
+-----------------+--------------+----------------------+

.. code-block:: bash

    python cli.py create-project -n my_etl

This creates a new project folder in your sqlbucket projects, with the right
folder convention to work with sqlbucket.


Command: run-job
----------------

+--------------------+--------------+-------------------------------------------------------------------------+
| **parameter**      | **shortcut** | **description**                                                         |
+--------------------+--------------+-------------------------------------------------------------------------+
| ``--name``         | ``-n``       |  Name of new project                                                    |
+--------------------+--------------+-------------------------------------------------------------------------+
| ``--db``           | ``-b``       |  Database name                                                          |
+--------------------+--------------+-------------------------------------------------------------------------+
| ``--fstep``        | ``-fs``      |  Step number to start project from (starts at 1)                        |
+--------------------+--------------+-------------------------------------------------------------------------+
| ``--tstep``        | ``-ts``      |  Step number to stop project from (exclusive)                           |
+--------------------+--------------+-------------------------------------------------------------------------+
| ``--from_date``    | ``-f``       |  Placeholder to put a date "from" variable in your project              |
+--------------------+--------------+-------------------------------------------------------------------------+
| ``--to_date``      | ``-t``       |  Placeholder to put a date "to" variable in your project                |
+--------------------+--------------+-------------------------------------------------------------------------+
| ``--from_days``    | ``-fd``      |  Placeholder to put a date "from" variable by telling number of days ago|
+--------------------+--------------+-------------------------------------------------------------------------+
| ``--to_days``      | ``-td``      |  Placeholder to put a date "to" variable by telling number of days ago  |
+--------------------+--------------+-------------------------------------------------------------------------+
| ``--group``        | ``-g``       |  Name of the group to run from the ``order`` attribute in config.yaml   |
+--------------------+--------------+-------------------------------------------------------------------------+
| ``--isolation``    | ``-i``       |  Set isolation level in case default from DB is not what you want       |
+--------------------+--------------+-------------------------------------------------------------------------+
| ``--verbose``      | ``-v``       |  Show queries in terminal before running them                           |
+--------------------+--------------+-------------------------------------------------------------------------+
| ``--render``       | ``-r``       |  Will only render and print the queries in terminal without running them|
+--------------------+--------------+-------------------------------------------------------------------------+

Some examples:

To run your project only from the second query (skipping the first):

.. code-block:: bash

    python cli.py run-job -n my_etl -b my_db -fs 2


To run your project only from the second query, until step 4 exclusively
(only 2 and 3 in that case):

.. code-block:: bash

    python cli.py run-job -n my_etl -b my_db -fs 2 -ts 4


To create ``from`` and ``to`` variables, that you can use in SQL content
using ``{{ from }}`` and ``{{ to }}``:

.. code-block:: bash

    python cli.py run-job -n my_etl -b my_db -f "2019-11-01" -t "2019-11-10"


To create ``from`` as date of 3 days ago and ``to`` ton 1 day ago (yesterday):

.. code-block:: bash

    python cli.py run-job -n my_etl -b my_db -fd 3 -td 1


To run your project only for the group ``whatever`` if indicated as a
possible order in your config.yaml.

.. code-block:: bash

    python cli.py run-job -n my_etl -b my_db -g whatever



**Default values**

If from and to variables are not indicated it will generates 3 days ago as
``from``, and today as ``to`` as default values.



Command: run-integrity
----------------------

+-----------------+--------------+--------------------------------------------+
| **parameter**   | **shortcut** | **description**                            |
+-----------------+--------------+--------------------------------------------+
| ``--name``      | ``-n``       |  Name of new project                       |
+-----------------+--------------+--------------------------------------------+
| ``--db``        | ``-b``       |  Database name                             |
+-----------------+--------------+--------------------------------------------+
| ``--prefix``    | ``-p``       |  Prefix of the test you want to run        |
+-----------------+--------------+--------------------------------------------+
| ``--verbose``   | ``-v``       |  Display queries in terminal when running  |
+-----------------+--------------+--------------------------------------------+


To run every integrity checks query found in the integrity folder
of your project.

.. code-block:: bash

    python cli.py run-integrity -n my_etl -b my_db


To run queries starting with a specific prefix (useful if your organize
your integrity queries in folders and you want to only run queries from one
folder).

.. code-block:: bash

    python cli.py run-integrity -n my_etl -b my_db -p revenue/

This typically would run only the queries in a folder `revenue`.
