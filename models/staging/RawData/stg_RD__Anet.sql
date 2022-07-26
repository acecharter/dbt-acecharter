WITH
  assessment_ids AS (
    SELECT 
      AceAssessmentId,
      AssessmentNameShort AS AceAssessmentName,
      AssessmentSubject
    FROM {{ ref('stg_GSD__Assessments') }}
    WHERE AssessmentFamilyName = 'Achievement Network'
  ),

  anet AS (
    SELECT
      CAST(school_year AS INT64) AS school_year,
      CAST(school_id AS INT64) AS school_id,
      CAST(school_name AS STRING) AS school_name,
      CAST(school_system_name AS STRING) AS school_system_name,
      CAST(school_cluster AS STRING) AS school_cluster,
      CAST(student_id AS INT64) AS student_id,
      CAST(sas_id AS INT64) AS sas_id,
      CAST(sis_id AS INT64) AS sis_id,
      CAST(student_first_name AS STRING) AS student_first_name,
      CAST(student_middle_name AS STRING) AS student_middle_name,
      CAST(student_last_name AS STRING) AS student_last_name,
      CAST(enrollment_grade AS STRING) AS enrollment_grade,
      CAST(course AS STRING) AS course,
      DATE(birth_date) AS birth_date,
      CAST(gender AS STRING) AS gender,
      CAST(race AS STRING) AS race,
      CAST(frl_status AS INT64) AS frl_status,
      CAST(lep_status AS INT64) AS lep_status,
      CAST(sped_status AS INT64) AS sped_status,
      CAST(previous_state_level AS STRING) AS previous_state_level,
      CAST(previous_state_score AS INT64) AS previous_state_score,
      CAST(period AS STRING) AS period,
      CAST(teacher_first_name AS STRING) AS teacher_first_name,
      CAST(teacher_last_name AS STRING) AS teacher_last_name,
      CAST(cycle AS INT64) AS cycle,
      CAST(subject AS STRING) AS subject,
      CAST(sas_name AS STRING) AS sas_name,
      CAST(assessment_id AS STRING) AS assessment_id,
      CAST(assessment_name AS STRING) AS assessment_name,
      CAST(super_genre AS STRING) AS super_genre,
      CAST(genre AS STRING) AS genre,
      CAST(passage_id AS STRING) AS passage_id,
      CAST(passage_title AS STRING) AS passage_title,
      CAST(domain AS STRING) AS domain,
      CAST(cluster_designation AS STRING) AS cluster_designation,
      CAST(cluster_code AS STRING) AS cluster_code,
      CAST(cc_standard_code AS STRING) AS cc_standard_code,
      CAST(item_id AS STRING) AS item_id,
      CAST(question_id AS STRING) AS question_id,
      CAST(item_type AS STRING) AS item_type,
      CAST(question_position AS STRING) AS question_position,
      CAST(response AS STRING) AS response,
      CAST(correct_answer_value AS STRING) AS correct_answer_value,
      CAST(points_received AS INT64) AS points_received,
      CAST(points_possible AS INT64) AS points_possible
    FROM {{ source('RawData', 'Anet2021MachineScored')}}
  ),

  final AS (
    SELECT
      i.AceAssessmentId,
      i.AceAssessmentName,
      CASE WHEN a.school_id = 100966 THEN '0125617' END AS StateSchoolCode,
      a.*,
      CASE WHEN a.item_type = 'Open Response' THEN 'Teacher' ELSE 'Machine' END AS scored_by
    FROM anet AS a
    LEFT JOIN assessment_ids as i
    ON a.Subject = i.AssessmentSubject
  )

  SELECT * FROM final