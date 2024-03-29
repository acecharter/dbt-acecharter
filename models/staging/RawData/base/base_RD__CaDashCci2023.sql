select
    cast(cds as string) as Cds,
    rtype as RType,
    schoolname as SchoolName,
    districtname as DistrictName,
    countyname as CountyName,
    charter_flag as CharterFlag,
    coe_flag as CoeFlag,
    dass_flag as DassFlag,
    studentgroup as StudentGroup,
    studentgroup_pct as StudentGroupPct,
    currdenom as CurrDenom,
    currstatus as CurrStatus,
    statuslevel as StatusLevel,
    currnsizemet as CurrNSizeMet,
    curr_prep as CurrPrep,
    curr_prep_pct as CurrPrepPct,
    curr_prep_summative as CurrPrepSummative,
    curr_prep_summative_pct as CurrPrepSummativePct,
    curr_prep_apexam as CurrPrepApExam,
    curr_prep_apexam_pct as CurrPrepApExamPct,
    curr_prep_ibexam as CurrPrepIbExam,
    curr_prep_ibexam_pct as CurrPrepIbExamPct,
    curr_prep_collegecredit as CurrPrepCollegeCredit,
    curr_prep_collegecredit_pct as CurrPrepCollegeCreditPct,
    curr_prep_agplus as CurrPrepAgPlus,
    curr_prep_agplus_pct as CurrPrepAgPlusPct,
    curr_prep_cteplus as CurrPrepCtePlus,
    curr_prep_cteplus_pct as CurrPrepCtePlusPct,
    curr_prep_ssb as CurrPrepSsb,
    curr_prep_ssb_pct as CurrPrepSsbPct,
    curr_prep_milsci as CurrPrepMilSci,
    curr_prep_milsci_pct as CurrPrepMilSciPct,
    curr_prep_reg_pre as CurrPrepRegPre,
    curr_prep_reg_pre_pct as CurrPrepRegPrePct,
    curr_prep_non_reg_pre as CurrPrepNonRegPre,
    curr_prep_non_reg_pre_pct as CurrPrepNonRegPrePct,
    curr_prep_non_reg_pre_DASS as CurrPrepNonRegPreDass,
    curr_prep_non_reg_pre_DASS_pct as CurrPrepNonRegPreDassPct,
    curr_prep_statefedjobs_DASS as CurrPrepStateFedJobsDass,
    curr_prep_statefedjobs_DASS_pct as CurrPrepStateFedJobsDassPct,
    curr_prep_trans_classwk as CurrPrepTransClassWk,
    curr_prep_trans_classwk_pct as CurrPrepTransClassWkPct,
    curr_aprep as CurrAPrep,
    curr_aprep_pct as CurrAPrepPct,
    curr_aprep_summative as CurrAPrepSummative,
    curr_aprep_summative_pct as CurrAPrepSummativePct,
    curr_aprep_collegecredit as CurrAPrepCollegeCredit,
    curr_aprep_collegecredit_pct as CurrAPrepCollegeCreditPct,
    curr_aprep_ag as CurrAPrepAg,
    curr_aprep_ag_pct as CurrAPrepAgPct,
    curr_aprep_cte as CurrAPrepCte,
    curr_aprep_cte_pct as CurrAPrepCtePct,
    curr_aprep_milsci as CurrAPrepMilSci,
    curr_aprep_milsci_pct as CurrAPrepMilSciPct,
    curr_aprep_non_reg_pre as CurrAPrepNonRegPre,
    curr_aprep_non_reg_pre_pct as CurrAPrepNonRegPrePct,
    curr_aprep_statefedjobs_DASS as CurrAPrepStateFedJobsDass,
    curr_aprep_statefedjobs_DASS_pct as CurrAPrepStateFedJobsDassPct,
    curr_aprep_trans_classwk as CurrAPrepTransClassWk,
    curr_aprep_trans_classwk_pct as CurrAPrepTransClassWkPct,
    curr_nprep as CurrNPrep,
    curr_nprep_pct as CurrNPrepPct,
    indicator as Indicator,
    reportingyear as ReportingYear
from {{ source('RawData', 'CaDashCci2023') }}
where
    rtype = 'X'
    --ESUHSD (includes ACE Charter High)
    or substr(cast(cds as string), 1, 7) = '4369427'
