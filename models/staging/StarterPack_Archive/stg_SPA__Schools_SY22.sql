SELECT
    '2021-22' AS SchoolYear,
    * 
FROM {{ source('StarterPack_Archive', 'Schools_SY22')}}