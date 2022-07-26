SELECT
  CAST(cds AS STRING) AS Cds,
  rtype AS RType,
  schoolname AS SchoolName,
  districtname AS DistrictName,
  countyname AS CountyName,
  charter_flag AS CharterFlag,
  coe_flag AS CoeFlag,
  dass_flag AS DassFlag,
  studentgroup AS StudentGroup,
  studentgroup_pct AS StudentGroupPct,
  currdenom AS CurrDenom,
  currstatus AS CurrStatus,
  priordenom AS PriorDenom,
  priorstatus AS PriorStatus,
  change AS Change,
  statuslevel AS StatusLevel,
  changelevel AS ChangeLevel,
  color AS Color,
  box AS Box,
  curr_prep AS CurrPrep,
  curr_prep_pct AS CurrPrepPct,
  curr_prep_summative AS CurrPrepSummative,
  curr_prep_summative_pct AS CurrPrepSummativePct,
  curr_prep_apexam AS CurrPrepApExam,
  curr_prep_apexam_pct AS CurrPrepApExamPct,
  curr_prep_ibexam AS CurrPrepIbExam,
  curr_prep_ibexam_pct AS CurrPrepIbExamPct,
  curr_prep_collegecredit AS CurrPrepCollegeCredit,
  curr_prep_collegecredit_pct AS CurrPrepCollegeCreditPct,
  curr_prep_agplus AS CurrPrepAgPlus,
  curr_prep_agplus_pct AS CurrPrepAgPlusPct,
  curr_prep_cteplus AS CurrPrepCtePlus,
  curr_prep_cteplus_pct AS CurrPrepCtePlusPct,
  curr_prep_ssb AS CurrPrepSsb,
  curr_prep_ssb_pct AS CurrPrepSsbPct,
  curr_prep_milsci AS CurrPrepMilSci,
  curr_prep_milsci_pct AS CurrPrepMilSciPct,
  curr_aprep AS CurrAPrep,
  curr_aprep_pct AS CurrAPrepPct,
  curr_aprep_summative AS CurrAPrepSummative,
  curr_aprep_summative_pct AS CurrAPrepSummativePct,
  curr_aprep_collegecredit AS CurrAPrepCollegeCredit,
  curr_aprep_collegecredit_pct AS CurrAPrepCollegeCreditPct,
  curr_aprep_ag AS CurrAPrepAg,
  curr_aprep_ag_pct AS CurrAPrepAgPct,
  curr_aprep_cte AS CurrAPrepCte,
  curr_aprep_cte_pct AS CurrAPrepCtePct,
  curr_aprep_milsci AS CurrAPrepMilSci,
  curr_aprep_milsci_pct AS CurrAPrepMilSciPct,
  curr_nprep AS CurrNPrep,
  curr_nprep_pct AS CurrNPrepPct,
  prior_prep AS PriorPrep,
  prior_prep_pct AS PriorPrepPct,
  prior_prep_summative AS PriorPrepSummative,
  prior_prep_summative_pct AS PriorPrepSummativePct,
  prior_prep_apexam AS PriorPrepApExam,
  prior_prep_apexam_pct AS PriorPrepApExamPct,
  prior_prep_ibexam AS PriorPrepIbExam,
  prior_prep_ibexam_pct AS PriorPrepIbExamPct,
  prior_prep_collegecredit AS PriorPrepCollegeCredit,
  prior_prep_collegecredit_pct AS PriorPrepCollegeCreditPct,
  prior_prep_agplus AS PriorPrepAgPlus,
  prior_prep_agplus_pct AS PriorPrepAgPlusPct,
  prior_prep_cteplus AS PriorPrepCtePlus,
  prior_prep_cteplus_pct AS PriorPrepCtePlusPct,
  prior_prep_ssb AS PriorPrepSsb,
  prior_prep_ssb_pct AS PriorPrepSsbPct,
  prior_prep_milsci AS PriorPrepMilSci,
  prior_prep_milsci_pct AS PriorPrepMilSciPct,
  prior_aprep AS PriorAPrep,
  prior_aprep_pct AS PriorAPrepPct,
  prior_aprep_summative AS PriorAPrepSummative,
  prior_aprep_summative_pct AS PriorAPrepSummativePct,
  prior_aprep_collegecredit AS PriorAPrepCollegeCredit,
  prior_aprep_collegecredit_pct AS PriorAPrepCollegeCreditPct,
  prior_aprep_ag AS PriorAPrepAg,
  prior_aprep_ag_pct AS PriorAPrepAgPct,
  prior_aprep_cte AS PriorAPrepCte,
  prior_aprep_cte_pct AS PriorAPrepCtePct,
  prior_aprep_milsci AS PriorAPrepMilSci,
  prior_aprep_milsci_pct AS PriorAPrepMilSciPct,
  prior_nprep AS PriorNPrep,
  prior_nprep_pct AS PriorNPrepPct,
  reportingyear AS ReportingYear
FROM {{ source('RawData', 'CaDashCci2018')}}
WHERE 
    rtype = 'X'
    OR rtype = 'D' AND cds = 43694270000000 --ESUHSD
    OR rtype = 'S' AND cds = 43694274330031 --Independence HS
    OR rtype = 'S' AND cds = 43694270125617 --ACE Charter HS
    