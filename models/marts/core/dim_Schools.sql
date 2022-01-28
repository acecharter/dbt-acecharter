WITH starter_pack_schools AS (
  SELECT * 
  FROM {{ ref('stg_StarterPack__Schools') }}
),

raw_data_schools AS (
  SELECT *
  FROM {{ ref('stg_RawData__Schools') }}
),

final AS (
  SELECT
    sp.SchoolId,
    rd.StateCdsCode,
    rd.StateCountyCode,
    rd.StateDistrictCode,
    rd.StateSchoolCode,
    sp.SchoolName,
    rd.SchoolNameMid,
    rd.SchoolNameShort,
    rd.SchoolType,
    sp.PhysicalStreetNumberName,
    sp.PhysicalCity,
    sp.PhysicalStateAbbreviation,
    sp.PhysicalPostalCode,
    sp.MailingStreetNumberName,
    sp.MailingCity,
    sp.MailingStateAbbreviation,
    sp.MailingPostalCode,
    rd.CurrentCharterTermStartDate,
    rd.CurrentCharterTermEndDate,
    rd.YearOpened,
    rd.PreviousRenewalYears,
    sp.GradeLevel,
    rd.GradesServed,
    rd.Grade5,
    rd.Grade6,
    rd.Grade7,
    rd.Grade8,
    rd.Grade9,
    rd.Grade10,
    rd.Grade11,
    rd.Grade12

  FROM starter_pack_schools AS sp
  LEFT JOIN raw_data_schools AS rd
  USING (SchoolId)
)


SELECT * FROM final
