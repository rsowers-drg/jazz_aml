/***************************************************************
  Program:       get_AML_patients.sql
  Author:        Ryan H. Sowers (rsowers@teamdrg.com)
 
  Creation Date: <2017-08-11 21:09:29>
  Time-stamp:    <2017-08-12 23:37:49>
  
  Pull All AML patients

 ***************************************************************/

-->| ############################################################
-->| ** CONDOR: 
-->| ** Codes in sandbox.jazz_aml_diag_code_list
-->| ############################################################

drop table if exists sandbox.jazz_condor_aml_patient_list;

create table sandbox.jazz_condor_aml_patient_list as select 
     distinct patient_id
from 
(select concat(trim(claims.patient_suffix), trim(claims.member_adr_zip)) as patient_id 
 from rwd.claim_record as claims
 inner join sandbox.jazz_aml_diag_code_list as codelist on
 (upper(claims.admit_diagnosis)   = upper(codelist.diag_code) or
  upper(claims.primary_diagnosis) = upper(codelist.diag_code) or
  upper(claims.diagnosis_code_2)  = upper(codelist.diag_code) or
  upper(claims.diagnosis_code_3)  = upper(codelist.diag_code) or
  upper(claims.diagnosis_code_4)  = upper(codelist.diag_code) or
  upper(claims.diagnosis_code_5)  = upper(codelist.diag_code) or
  upper(claims.diagnosis_code_6)  = upper(codelist.diag_code) or
  upper(claims.diagnosis_code_7)  = upper(codelist.diag_code) or
  upper(claims.diagnosis_code_8)  = upper(codelist.diag_code))) as amlclaims;

-->| ############################################################
-->| ** ALBATROSS
-->| ** Codes in sandbox.jazz_aml_diag_code_list
-->| ############################################################

drop table if exists sandbox.jazz_albatross_aml_patient_list;

create table sandbox.jazz_albatross_aml_patient_list as select
     distinct patient_id
from 
(select concat(claims.patientsuffix, claims.patientzipcode) as patient_id
 from rwd.claims_header as claims
 inner join sandbox.jazz_aml_diag_code_list as codelist on
 (upper(claims.diagnosiscode_1) = upper(codelist.diag_code) or
  upper(claims.diagnosiscode_2) = upper(codelist.diag_code) or
  upper(claims.diagnosiscode_3) = upper(codelist.diag_code) or
  upper(claims.diagnosiscode_4) = upper(codelist.diag_code) or
  upper(claims.diagnosiscode_5) = upper(codelist.diag_code) or
  upper(claims.diagnosiscode_6) = upper(codelist.diag_code) or
  upper(claims.diagnosiscode_7) = upper(codelist.diag_code) or
  upper(claims.diagnosiscode_8) = upper(codelist.diag_code))) as amlclaims;


-->| ############################################################
-->| ** VULTURE
-->| ** Codes in sandbox.jazz_aml_diag_code_list
-->| ############################################################

drop table if exists sandbox.jazz_vulture_aml_patient_list;

create table sandbox.jazz_vulture_aml_patient_list as select
     distinct patlist.patient_id
from (select distinct claims.claimid from rwd.ability_vwdiagnosis as claims
      inner join sandbox.jazz_aml_diag_code_list as codelist on
      (replace(upper(trim(diagnosiscode)),'.','') = codelist.diag_code)) as claimlist
inner join (select concat(coalesce(key1, key2, key3), trim(zip3)) as patient_id, claimid
            from rwd.ability_vwpatient) as patlist on
(claimlist.claimid = patlist.claimid);


-->| ############################################################
-->| ** Create combined patient list
-->| ############################################################

drop table if exists sandbox.jazz_aml_patient_list_raw;

create table sandbox.jazz_aml_patient_list_raw as select
     distinct patlist.patient_id
from 
(select patient_id from sandbox.jazz_condor_aml_patient_list union
 select patient_id from sandbox.jazz_albatross_aml_patient_list) as patlist;

drop table if exists sandbox.jazz_condor_aml_patient_list;
drop table if exists sandbox.jazz_albatross_aml_patient_list;

-->| ############################################################
-->| ** Get all identified patient claims
-->| ############################################################

drop table if exists sandbox.jazz_aml_condor_claims;

create table sandbox.jazz_aml_condor_claims as select
     concat(trim(claims.patient_suffix), trim(claims.member_adr_zip)) as patient_id,
     claim_number,
     coalesce(statement_from, admission_date, statement_to) as claim_date,
     coalesce(attending_pr_npi, billing_pr_npi, referring_pr_npi) as npi,
     claim_type_code,
     admit_diagnosis,
     primary_diagnosis,
     diagnosis_code_2,
     diagnosis_code_3,
     diagnosis_code_4,
     diagnosis_code_5,
     diagnosis_code_6,
     diagnosis_code_7,
     diagnosis_code_8,
     principal_procedure,
     other_proc_code_2,
     other_proc_code_3,
     other_proc_code_4,
     other_proc_code_5,
     other_proc_code_6,
     other_proc_code_7,
     other_proc_code_8,
     other_proc_code_9,
     other_proc_code_10
from rwd.claim_record as claims
inner join sandbox.jazz_aml_patient_list_raw as patlist on
(concat(trim(claims.patient_suffix), trim(claims.member_adr_zip)) = patlist.patient_id);

drop table if exists sandbox.jazz_aml_albatross_claims;

create table sandbox.jazz_aml_albatross_claims as select 
     concat(claims.patientsuffix, claims.patientzipcode) as patient_id,
     to_varchar(claims.entityid) as claim_number,
     to_date(coalesce(detail.servicefromdate, claims.createdate)) as claim_date,
     coalesce(claims.renderingprovidernpi,claims.billingprovnpi,claims.referringprovnpi) as npi,
     detail.StdChgLineHCPCSProcedureCode as procedure_1,
     detail.dmechglinehcpcsprocedurecode as procedure_2,
     claims.diagnosiscode_1,
     claims.diagnosiscode_2,
     claims.diagnosiscode_3,
     claims.diagnosiscode_4,
     claims.diagnosiscode_5,
     claims.diagnosiscode_6,
     claims.diagnosiscode_7,
     claims.diagnosiscode_8
from rwd.claims_header as claims
inner join sandbox.jazz_aml_patient_list_raw as patlist on
(concat(claims.patientsuffix, claims.patientzipcode) = patlist.patient_id)
left join rwd.claims_detail as detail on
(claims.entityid = detail.entityid);

drop table if exists sandbox.jazz_aml_vulture_claims;

create table sandbox.jazz_aml_vulture_claims as select
     claimlist.patient_id,
     claimlist.claimid as claim_number,
     try_to_date(coalesce(nullif(serv.servicestart, 'NULL'), nullif(serv.processdate, 'NULL'), nullif(claimlist.processdate, 'NULL'))) claim_date,
     nullif(upper(trim(diag.diagnosiscode)), 'null') as diagnosis_code_0,
     serv.drugcode as ndc,
     nullif(upper(trim(serv.procedurecode)), 'null') as procedure_1
from 
(select distinct patlist.patient_id, claims.claimid, claims.processdate
 from sandbox.jazz_vulture_aml_patient_list as patlist
 inner join rwd.ability_vwpatient as claims on
 (patlist.patient_id = concat(coalesce(key1, key2, key3), trim(zip3)))
 where claims.claimid is not NULL and 
       claims.processdate is not NULL and 
       patlist.patient_id is not NULL) as claimlist
left join rwd.ability_vwdiagnosis as diag on (claimlist.claimid = diag.claimid)
left join rwd.ability_vwserviceline as serv on (claimlist.claimid = serv.claimid);


-->| ############################################################
-->| ** Stack the claims
-->| ############################################################

drop table if exists sandbox.jazz_aml_full_claims_raw;

create table sandbox.jazz_aml_full_claims_raw as select
     distinct
     claims.patient_id,
     claims.claim_number,
     claims.claim_date,
     claims.npi,
     claims.source,
     claims.code_type,
     claims.code,
     codelist.aml_group
from
(
 -->| Condor diagnoses
select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'diag' as code_type,
	admit_diagnosis as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'diag' as code_type,
	primary_diagnosis as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'diag' as code_type,
	diagnosis_code_2 as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'diag' as code_type,
	diagnosis_code_3 as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'diag' as code_type,
	diagnosis_code_4 as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'diag' as code_type,
	diagnosis_code_5 as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'diag' as code_type,
	diagnosis_code_6 as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'diag' as code_type,
	diagnosis_code_7 as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'diag' as code_type,
	diagnosis_code_8 as code
 from sandbox.jazz_aml_condor_claims union all

 -->| Condor procedures
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'proc' as code_type,
	principal_procedure as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'proc' as code_type,
	other_proc_code_2 as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'proc' as code_type,
	other_proc_code_3 as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'proc' as code_type,
	other_proc_code_4 as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'proc' as code_type,
	other_proc_code_5 as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'proc' as code_type,
	other_proc_code_6 as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'proc' as code_type,
	other_proc_code_7 as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'proc' as code_type,
	other_proc_code_8 as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'proc' as code_type,
	other_proc_code_9 as code
 from sandbox.jazz_aml_condor_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'CONDOR' as source,
	'proc' as code_type,
	other_proc_code_10 as code
 from sandbox.jazz_aml_condor_claims union all

 --> Albatross diagnoses
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'ALBATROSS' as source,
	'diag' as code_type,
	diagnosiscode_1 as code
 from sandbox.jazz_aml_albatross_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'ALBATROSS' as source,
	'diag' as code_type,
	diagnosiscode_2 as code
 from sandbox.jazz_aml_albatross_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'ALBATROSS' as source,
	'diag' as code_type,
	diagnosiscode_3 as code
 from sandbox.jazz_aml_albatross_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'ALBATROSS' as source,
	'diag' as code_type,
	diagnosiscode_4 as code
 from sandbox.jazz_aml_albatross_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'ALBATROSS' as source,
	'diag' as code_type,
	diagnosiscode_5 as code
 from sandbox.jazz_aml_albatross_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'ALBATROSS' as source,
	'diag' as code_type,
	diagnosiscode_6 as code
 from sandbox.jazz_aml_albatross_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'ALBATROSS' as source,
	'diag' as code_type,
	diagnosiscode_7 as code
 from sandbox.jazz_aml_albatross_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'ALBATROSS' as source,
	'diag' as code_type,
	diagnosiscode_8 as code
 from sandbox.jazz_aml_albatross_claims union all

 --> Albatross procedures
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'ALBATROSS' as source,
	'proc' as code_type,
	procedure_1 as code
 from sandbox.jazz_aml_albatross_claims union all
 select patient_id, 
	claim_number, 
	claim_date, 
	npi,
	'ALBATROSS' as source,
	'proc' as code_type,
	procedure_2 as code
 from sandbox.jazz_aml_albatross_claims union all
 select patient_id,
 	claim_number,
	claim_date,
	'' as npi,
	'VULTURE' as source,
	'diag' as code_type,
	diagnosis_code_0 as code
 from sandbox.jazz_aml_vulture_claims union all
 select patient_id,
 	claim_number,
	claim_date,
	'' as npi,
	'VULTURE' as source,
	'proc' as code_type,
	procedure_1 as code
 from sandbox.jazz_aml_vulture_claims) as claims
left join sandbox.jazz_aml_diag_code_list as codelist on 
(claims.code = codelist.diag_code)
where claims.code is not NULL
order by claims.patient_id, claims.claim_date, claims.claim_number;

-->| ############################################################
-->| ** Create the flagged patient list
-->| ############################################################

drop table if exists sandbox.jazz_aml_flagged_pat_list;

create table sandbox.jazz_aml_flagged_pat_list as select
     patient_id,
     min(claim_date) as first_claim_date,
     max(claim_date) as last_claim_date,
     count(*) as claim_count,

     count(distinct case when upper(code_type) = 'DIAG' then claim_number end) as dx_claim_count,
     count(distinct case when upper(code_type) = 'PROC' then claim_number end) as px_claim_count,

     (case when count(case when aml_group is not NULL then claim_number end)>0 then 1 end) as ml_any,
     min(case when aml_group is not NULL then claim_date end) as ml_any_init_date,

     (case when count(case when aml_group ilike 'AML' then claim_number end)>0 then 1 end) as aml_any,
     min(case when aml_group ilike 'AML' then claim_date end) as aml_any_init_date,

     (case when count(case when aml_group = 'AML' then claim_number end)>0 then 1 end) as aml,
     min(case when aml_group = 'AML' then claim_date end) as aml_init_date,
     
     (case when count(case when aml_group = 'AML_ABN' then claim_number end)>0 then 1 end) as aml_abn,
     min(case when aml_group = 'AML_ABN' then claim_date end) as aml_abn_init_date,
     
     (case when count(case when aml_group = 'AML_DYSP' then claim_number end)>0 then 1 end) as aml_dysp,
     min(case when aml_group = 'AML_DYSP' then claim_date end) as aml_init_dysp_date,
     
     (case when count(case when aml_group = 'OTHER_ML' then claim_number end)>0 then 1 end) as other_ml,
     min(case when aml_group = 'OTHER_ML' then claim_date end) as other_ml_init_date,
     
     (case when count(case when aml_group = 'UNSPEC_ML' then claim_number end)>0 then 1 end) as unspec_ml,
     min(case when aml_group = 'UNSPEC_ML' then claim_date end) as unspec_ml_init_date

from sandbox.jazz_aml_full_claims_raw
group by patient_id;


-->| ############################################################
-->| ** Check monthly counts
-->| ############################################################

select
     list.month,
     count(distinct list.patient_id) as pat_count
from 
     (select 
     ((year(aml_any_init_date)*100)+month(aml_any_init_date)) as month,
     patient_id
     from sandbox.jazz_aml_flagged_pat_list
     where aml_any = 1 and datediff(months,first_claim_date,aml_any_init_date)>=1) as list
group by list.month
order by list.month;


