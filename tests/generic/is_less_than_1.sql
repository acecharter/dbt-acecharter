-- Most rate calculations should have a value < 100%. Therefore return records where this isn't true to make the test fail.
{% test is_less_than_1(model, rate_column_name) %}
  select *
  from {{ model}}
  where {{ column_name}} > 1

{% endtest %}