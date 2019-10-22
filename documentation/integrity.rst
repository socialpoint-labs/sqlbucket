Data quality and integrity with SQLBucket
=========================================


On top of writing ETL, SQLBucket helps you organize some integrity checks you
may need on your ETL. The SQL for integrity checks has only one convention: it
must returns the result of your integrity as a boolean on the alias field
**passed**. Everything is done in plain and simple SQL. You can have as many
other fields as you want.

The integrity SQL must reside in .sql files in the ``integrity`` folder within
a project.

You will find below some examples on how they should be written.


**No nulls in field**

If we were to guarantee a field does not contain any nulls, the following query
would check just that.

.. code-block:: sql

    select
        count(1) = 0 as passed
    from my_table
    where my_field is null


When an integrity fails, it will log the rows of your queries in terminal, so
you can easily review the failures. It is advised to actually put the result
calculated as well as the result expected in the query on their separate fields.
For the same check, the integrity query would look as follow:


.. code-block:: sql

    select
        0 as expected,
        count(1) as calculated,
        count(1) = 0 as passed
    from my_table
    where my_field is null


**Expected number of rows**

.. code-block:: sql

    select
        10 as expected,
        count(1) as calculated,
        count(1) = 10 as passed
    from my_table


**Non-negative values**

.. code-block:: sql

    select
        0 as expected,
        count(1) as calculated,
        count(1) = 0 as passed
    from my_table
    where positive_field < 0


**Only specific values**

.. code-block:: sql

    select
        0 as expected,
        count(1) as calculated,
        count(1) = 0 as passed
    from my_table
    where field not in ('high', 'low')


**Values must be unique**

.. code-block:: sql

    select
        count(distinct(my_field)) as expected,
        count(1) as calculated,
        count(1) = count(distinct(my_field)) as passed
    from my_table



Organization and best practice
------------------------------

The folder organization of integrity queries is completely up to the user, as
long as they are all contained in the ``integrity`` folder, and returns the
field ``passed`` as a boolean.

It is also possible to have multiple checks within one SQL file, simply
by using ``union all`` on the query. This is how it could be done:


.. code-block:: sql

    select
        'field must be unique' as integrity_name,
        count(1) = count(distinct(my_field)) as passed,
        count(distinct(my_field)) as expected,
        count(1) as calculated
    from my_table

    union all

    select
        'only values high/low' as integrity_name,
        count(1) = 0 as passed,
        0 as expected,
        count(1) as calculated
    from my_table
    where positive_field in ('high', 'low')



More advanced
-------------

You may want to check that the aggregation from your ETL worked as intended,
and make sure that the data from your raw data tables equals the aggregation
in the summary tables.

The following integrity SQL could be done as follow:

.. code-block:: sql

    select
        (select sum(revenue) from raw_table) as source,
        (select sum(revenue) from summary_table) as target,
        (select sum(revenue) from raw_table)
            ==
        (select sum(revenue) from summary_table) as passed

This could work if revenues were only integers. However, in real life, cost or
revenues are reported in the float/double precision form. The above check
therefore is not going to work most likely.

For comparing floats you could do it this way:

.. code-block:: sql

    select
        (select sum(revenue) from raw_table) as source,
        (select sum(revenue) from summary_table) as target,
        abs((select sum(revenue) from raw_table) - (select sum(revenue) from summary_table)) < 1 as passed

This integrity check will pass if the difference between the 2 sums is under 1.
It could be set to something smaller if needed.


Tricks with Jinja2
------------------

SQLBucket works with Jinja2 under the hood which means you can have for loops
or if/else execution flows in your integrity checks, as well as, create some
macros. See below some examples:

**No nulls in multiple fields**

.. code-block:: sql

    {% set fields = ['field_a', 'field_b', 'field_c', 'field_d', 'field_e'] %}

    {% for field in fields %}
    select
        {{ field }} as '{{ field }}',
        0 as expected,
        count(1) as calculated,
        count(1) = 0 as passed
    from my_table
    where {{ field }} is null

    {{ "union all" if not loop.last }}

    {% endfor %}

This will effectively create 5 checks into one query.


**No nulls in multiple fields via macro**

When creating your SQLBucket object, you can pass as a parameter a folder path
for jinja macros.

.. code-block:: python

    bucket = SQLBucket(macro_folder='/path/to/macro/folder')

This gives you the opportunity to create Jinja macros to simplify your code
when doing integrity. To keep the same example as above, we could create the
following macro to check nulls in multiple fields from a table.


.. code-block:: jinja

    {% macro check_no_nulls(table_name, list_of_fields) %}

        {% for field in list_of_fields %}
        select
            {{ field }} as '{{ field }}',
            0 as expected,
            count(1) as calculated,
            count(1) = 0 as passed
        from {{ table_name }}
        where {{ field }} is null

        {{ "union all" if not loop.last }}

        {% endfor %}

    {% endmacro %}


Then, for every tables you want to check for nulls, it would take only the
following (assuming the macros is written on a file called macros.sql):


.. code-block:: jinja

    {% import 'macros.sql' as macros %}

    {{ macros.check_no_nulls('table_name', ['field_a', 'field_b', 'field_c','field_d'])}}


Using macros is an excellent way to prevent writing the same SQL over and over.
SQLBucket library at the moment contains only one macro, which gives the
possibility to compare two metrics, and make sure that their difference is
within a threshold.



.. code-block:: jinja

    {% import 'macros.j2' as macros %}

    {% set sum_source = 'select sum(revenue) from source_table' %}
    {% set sum_target = 'select sum(revenue) from target_table' %}

    {{ macros.are_within_threshold(sum_source, sum_target, 0.01) }}


This will generate an SQL query that will return True if the sum of the target
table is within the threshold given (in this case 1%).

