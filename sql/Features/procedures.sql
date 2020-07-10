select 
    a.person_id,
    a.procedure_concept_id || ' - procedure - ' || coalesce (
        c.concept_name, 'no match'
    ) as concept_name,
    cast(a.procedure_datetime as DATE) as feature_start_date,
    b.start_date as person_start_date,
    b.end_date as person_end_date
from 
    {cdm_schema}.procedure_occurrence a
inner join
    {cohort_table} b
on 
    a.person_id = b.person_id
left join
    {cdm_schema}.concept c
on 
    c.concept_id = a.procedure_concept_id


