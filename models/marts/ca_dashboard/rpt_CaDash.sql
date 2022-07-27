WITH
  dash AS (
    SELECT * FROM {{ref ('int_CaDashAll')}}
  ),

SELECT * FROM dash