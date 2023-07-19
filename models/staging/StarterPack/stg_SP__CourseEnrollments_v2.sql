select
    case
        when extract(month from BeginDate) > 7
            then concat(
                extract(year from BeginDate),
                '-',
                substr(cast((extract(year from BeginDate) + 1) as string), 3, 2)
            )
        when extract(month from BeginDate) <= 7
            then concat(
                extract(year from BeginDate) - 1,
                '-',
                extract(year from BeginDate) - 2000
            )
        else 'ERROR'
    end as SchoolYear,
    *
from {{ source('StarterPack', 'CourseEnrollments_v2') }}
