WITH
  schools AS (
    SELECT 
      SchoolId,
      SchoolName,
      SchoolNameMid,
      SchoolNameShort
    FROM {{ref('dim_Schools')}}
  ),

  students AS (
    SELECT * EXCEPT(
      LastName,
      FirstName,
      MiddleName,
      BirthDate,
      Email
    )
    FROM {{ref('dim_Students')}}
  ),
  
  attendance AS (
    SELECT * FROM {{ ref('fct_StudentAttendance')}}
  ),

  final AS (
    SELECT
      sc.*,
      st.* EXCEPT (SchoolId),
      a.* EXCEPT (SchoolId, StudentUniqueId)
    FROM attendance AS a
    LEFT JOIN schools AS sc
    ON a.SchoolId = sc.SchoolId
    LEFT JOIN students AS st
    ON
      a.SchoolId = st.SchoolId
      AND a.StudentUniqueId = st.StudentUniqueId
  )

SELECT * FROM final