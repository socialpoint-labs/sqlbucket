Using variables in SQLBucket
============================


One of the key component of SQLBucket is the ability to set variables and render
them in your SQL using the Jinja2 library. While you could use SQLBucket without
using variables at all, this feature offers great flexibility when writing a
SQL ETL.

For instance, you could have an identical SQL query that selects the same fields
with the same filters, but on different tables.

You could write the following SQL once, and then assign a different value to
the variable ``table``.

.. code-block:: jinja

    SELECT some_field from {{ table }}

Here, ``table`` is the variable name, and you can assign any value to it.


Assigning values to variables
-----------------------------

There are 3 types of variables in SQLBucket:

* global variables
* connection variables
* project variables

Each one is independent of the other (meaning you can use one without the other).
They are all optionals.

Global and connections variables are passed over as parameters when creating
your SQLBucket object in Python.

Project variables, just as they're called, are set when you load a project (in
Python and also via CLI). The most common project variables we have seen are
dates for incremental queries, but it can be anything you want.


Global variables
----------------

Global variables can be created when instantiating your SQLBucket object. Those
variables will be accessible by every project and are particularly useful when
setting different environment (production, local, etc). This is completely
optional to use.

.. code-block:: python

    from sqlbucket import SQLBucket


    my_global_variables = {
        'foo': 'bar'
    }

    bucket = SQLBucket(global_variables=my_global_variables)

You can access global variables in your SQL queries by preceding the variable
name with ``g``.


.. code-block:: sql

    SELECT my_field FROM {{ g.foo }}

In that case, the ``g.foo`` variable will be rendered as ``bar``.


This is perfect tool also for setting different search_path if the project runs
from your computer or in production.

.. code-block:: python

    # local environment
    local_global_variables = {
        'schema_suffix': '_staging'
    }

    # production environment
    prod_global_variables = {
        'schema_suffix': ''
    }

You could then set a search path at the beginning of your ETL:

.. code-block:: sql

    set search_path to my_schema{{ g.schema_suffix }};

    -- this would be rendered as
    set search_path to my_schema_staging; -- in local
    set search_path to my_schema; -- in production


Connection variables
--------------------

Connection variables are simply a set of variables you can add based on connection
names when running a project. When you a load project for a given connection,
SQLBucket checks if some connection variables matches the connection name submitted
and pass them over as values you can render on the SQL queries. 

This is optional to use.


.. code-block:: python

    from sqlbucket import SQLBucket

    my_connections = {
        'mydb': 'hive://username:password@host:port/databasename'
    }

    my_connection_variables = {
        'mydb': {
            'schema': 'my_schema_name',
            'foo': 'bar'
        }
    }

    bucket = SQLBucket(
        connections=my_connections,
        connection_variables=my_connection_variables
    )
    random_project = bucket.load_project(name='<some project>', connection='mydb')
    random_project.run()


You can access the connection variables by preceding them with ``c`` (for
connection) in your SQL queries.

.. code-block:: sql

    set search_path to {{ c.schema }};
    -- this will be rendered as
    set search_path to my_schema_name;

The name of the connection is present as the variable ``c.name``. This means
you can introduce some connection based logic in your SQL using Jinja2 execution
flow:

.. code-block:: jinja

    {% if c.name == 'mydb' %}
    DELETE FROM table WHERE whatever is NULL
    {% else %}
    DELETE FROM table
    {% endif %}



Project variables
-----------------

You set the project variables at project level. The most typical project
variables you would have are dates and datetime for incremental queries, but
it can be anything based on your needs.


.. code-block:: python

    from sqlbucket import SQLBucket
    from datetime import datetime


    bucket = SQLBucket()

    # building the project variables
    yesterday = datetime.now() - timedelta(days=1)
    today = datetime.now()

    project_variables = {
        'from': yesterday.strftime('%Y-%m-%d'),
        'to' today.strftime('%Y-%m-%d')
    }
    random_project = bucket.load_project(
        name='<some project>',
        connection='mydb',
        variables=project_variables
    )
    random_project.run()


You will then be able to access those variables as follows:

.. code-block::jinja

    DELETE FROM my_table
    WHERE datetime >= '{{ from }}' AND datetime < '{{ to }}'

Project variables are not preceded by any letter, as opposed to global and
connection variables.

The command line interface for running a project gives you plenty of options to
create date variables. You can refer to the CLI documentation.


Setting variables with config.yaml
----------------------------------

The config.yaml gives you the possibility to set variables in a more static way,
in case you need it. You can refer to the documentation about the project
configuration.
