WITH
  schools AS (
    SELECT 
      SchoolYear,
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
      BirthDate
    )
    FROM {{ref('dim_Students')}}
  ),
  
  attendance AS (
    SELECT * FROM {{ ref('fct_StudentAttendance')}}
  ),

  final AS (
    SELECT
      a.SchoolYear,
      sc.* EXCEPT (SchoolYear),
      st.* EXCEPT (SchoolYear, SchoolId),
      a.* EXCEPT (SchoolId, StudentUniqueId, SchoolYear)
    FROM attendance AS a
    LEFT JOIN schools AS sc
    ON
      a.SchoolId = sc.SchoolId
      AND a.SchoolYear = sc.SchoolYear
    LEFT JOIN students AS st
    ON
      a.SchoolId = st.SchoolId
      AND a.StudentUniqueId = st.StudentUniqueId
      AND a.SchoolYear = st.SchoolYear
  )

SELECT * FROM final