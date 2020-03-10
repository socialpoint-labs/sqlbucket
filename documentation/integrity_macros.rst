Common macros for integrity
===========================


For your integrity, SQLBucket has built a few Jinja2 SQL macros for common data
quality queries usually made by data engineers or data analysts. This should
save you time when starting with SQLBucket. This documentation page is an
attempt to describe those existing macros.


Take in consideration that each of those macros returns the 4 same fields:
 * integrity_check: name of the integrity.
 * expected: description or actual expected result.
 * calculated: the calculated result of your check.
 * passed: boolean result of expected = calculated.


The field names are consistent between macros, so you can `union all` macros
and return the results of many integrity checks from just one sql file. We will
use this practice for showing the currently available macros.


Common macros
-------------


.. code-block:: jinja

    {% import 'common.jinja2' as common %}


    {{ common.is_unique(table='users', column='id') }}
    union all
    {{ common.has_row_count(table='users', target_size=1000) }}
    union all
    {{ common.has_row_count_between(table='users', min_size=900, max_size=1100) }}
    union all
    {{ common.is_complete(table='users', column='id', threshold=1) }}
    union all
    {{ common.is_complete(table='users', column='id', threshold=0.8) }}
    union all
    {{ common.is_contained_in(table='users', column='country', values=['FR', 'ES', 'US']) }}
    union all
    {{ common.is_non_negative(table='users', column='age') }}

This will create a set of very common integrity checks on your `users` table.

A more important integrity check is to validate metrics across multiple tables.
Most ETL aggregates in various ways and we need to ensure that our ETL does not
leak or duplicates rows with nasty joins.

The only way to ensure integrity on aggregated metrics is simply by comparing
metrics between the source (raw) table and the target table (aggregated).



Source to target macro
----------------------
We have a macro just for that.


.. code-block:: jinja

    {% import 'common.jinja2' as common %}


    {% set revenue_source = 'select sum(revenue) from raw_table' %}
    {% set revenue_target = 'select sum(revenue) from aggregated_table' %}


    {{ common.are_within_threshold(revenue_source, revenue_target, threshold=0.01) }}


This will check both metrics are within a given difference (0.01% in this case).
If no threshold is indicated, it will expect perfect equality (careful when
comparing floats).




Aggregations macros
-------------------

.. code-block:: jinja

    {% import 'aggregations.jinja2' as aggs %}

    {{ aggs.column_max_to_be_between(table='users', column='age', lower_value=0, higher_value=100) }}
    union all
    {{ aggs.column_min_to_be_between(table='users', column='age', lower_value=0, higher_value=5) }}
    union all
    {{ aggs.column_sum_to_be_between(table='users', column='revenue', lower_value=100000, higher_value=200000) }}
    union all
    {{ aggs.column_mean_to_be_between(table='users', column='age', lower_value=40, higher_value=60) }}
    union all
    {{ aggs.column_median_to_be_between(table='users', column='age', lower_value=45, higher_value=55) }}
    union all
    {{ aggs.column_stdev_to_be_between(table='users', column='age', lower_value=30, higher_value=45) }}


This will make a few integrity checks on your table based on certain aggregations
