How to write SQL in SQLBucket
=============================


With SQLBucket, you can write your SQL plain and simple. The library is made in
such a way so that you can be up and running as soon as possible. Writing
normal SQL will be enough in most cases.

However, because SQLBucket uses Jinja2 as template engine, you can achieve much
more than just writing SQL. Using `for loops` and `if/else` execution flows can
help you make your code shorter and easier to manage.

On this page, we will cover how to get you started with an ETL, and add some
best practices advices.


Organize your queries and execution order
-----------------------------------------

The SQL queries that makes your ETL can be organized in the folder structure of
your choice, as long as they're written in SQL files in the folder `queries` of
your project.


**Setting the order of your queries**

To define in which order your files should be executed, you must add an `order`
attribute in your config.yml, and list the path of the queries in the order you
want them executed. See below an example:


.. code-block:: yaml

    order:
        - query_1.sql
        - query_2.sql
        - folder_1/query_3.sql

As you can see, you can have the folder structure of your choice, as long as
the right path is used in the `order` list.


**Grouping your queries**

In some cases you may want to run only a subset of your queries at specific time
You can create `group` within the `order` attribute as follow:

.. code-block:: yaml

    order:
        main:
            - query_1.sql
            - query_2.sql
        other
            - folder_1/query_3.sql

This is how your python code would look like thereafter:

.. code-block:: python

    bucket = SQLBucket(connections=connections)
        project = bucket.load_project(
            project_name='fat_etl',
            connection_name='db_test',
            variables={'foo': 1}
        )

        project.run(group='other')

That will run only the query within the group `other`.

If you do not specify a group but your config.yaml has the `order` in the group
form, it will search for the `main` group. If `main` does not exists, it will
raise an error.

**Setting variables**

The config file also permits you to set variables for your ETL, based on which
connection the ETL is running for.


.. code-block:: yaml

    variables:
        connection_name1:
            foo: bar
        connection_name2:
            foo: barbar

You need to set variables for every connection/db your ETL may run against.
Say you run your ETL on a connection named 'db1', to render variables set
from the config.yaml, you would need the following config.


.. code-block:: yaml

    variables:
        db1:
            my_field: foo
            my_table: bar

You could then use those variables in your SQL, assuming it runs for the
connection db1 in the following way:

.. code-block:: sql

    select {{ my_field }} from {{ my_table }};


You can also set variables via command line, or in Python code directly. For
more about variables, see the `documentation on setting up variables`_.



Writing SQL with Jinja
----------------------

SQLBucket, relying on Jinja2 library, automatically creates a Jinja environment
out of a project. What does it mean for you? You will see below a few things we
can do.


**using for loops**

Say you need to delete data because of GDPR. You could write the following
code in one of your query file:

.. code-block:: jinja

    {% set tables = ['table_a', 'table_b', 'table_c', 'table_d'] %}

    {% for table in tables %}

    DELETE FROM {{ table }} WHERE continent = 'EU' and age < 18;

    {% endfor %}

