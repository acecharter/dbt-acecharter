SELECT *
FROM {{ ref('int_Caaspp2021AllSantaClaraCounty_IDs_joined')}}
WHERE SchoolCode IN (
  '0116814', --ACE Empower
  '0125617', --ACE High School
  '0129247', --ACE Esperanza
  '0131656'  --ACE Inspire
)