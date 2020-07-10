/*
    Construct the cohort table used in an example End-of-Life prediction Model

    Inclusion criteria:
    - Enrolled in 95% of months of training
    - Enrolled in 95% of days during outcome window
*/

with
    mi_dates as (
        
        select
            person_id,
            mi_datetime
        from 
        (
            select
                person_id,
                condition_start_datetime as mi_datetime,
                row_number() over(partition by person_id order by condition_start_datetime) as rn
            from
                {omop_cdm_schema}.condition_occurrence co
            join 
                {omop_cdm_schema}.concept_ancestor ca
            on 
                co.condition_concept_id = ca.descendant_concept_id
            where ancestor_concept_id = 316866
        )a where rn = 1
    ),
    mi_training_elig_counts as (
        select
            o.person_id,
            o.observation_period_start_date as start,
            o.observation_period_end_date as finish,
            greatest(
                date_diff(least (
                    o.observation_period_end_date,
                    date '{training_end_date}'
                ), greatest(
                    o.observation_period_start_date,
                    date '{training_start_date}'
                ), DAY), 0
            ) as num_days
        from {omop_cdm_schema}.observation_period o
    ),
    mi_trainingwindow_elig_perc as (
        select
            person_id
        from
            mi_training_elig_counts
        group by
            person_id
        having
            sum(num_days) >= 0.95 * (date_diff(date '{training_end_date}', date '{training_start_date}', DAY))
    ),
    mi_testperiod_elig_counts as (
        select
            p.person_id,
            p.observation_period_start_date as start,
            p.observation_period_end_date as finish,
            greatest(
                    date_diff(least (
                        p.observation_period_end_date,
                        date_add(date_add(
                            date '{training_end_date}', INTERVAL {gap}), INTERVAL {outcome_window})
                    ), greatest(
                        p.observation_period_start_date,
                        date '{training_end_date}'
                    ), DAY), 0
            ) as num_days
        from {omop_cdm_schema}.observation_period p
        inner join 
            mi_trainingwindow_elig_perc tr
        on 
            tr.person_id = p.person_id
    ), 
    mi_testwindow_elig_perc as (
        select
            person_id
        from
            mi_testperiod_elig_counts
        group by 
            person_id
        having
            sum(num_days) >= 0.95 * date_diff(date_add(date_add(DATE '1900-01-01', INTERVAL {gap}), INTERVAL {outcome_window}),
                                                                DATE '1900-01-01', DAY)
    ) 
    
    select
        row_number() over (order by te.person_id) - 1 as example_id,
        te.person_id,
        date '{training_start_date}' as start_date,
        date '{training_end_date}' as end_date,
        cast(d.mi_datetime as DATE) as outcome_date,
        
        cast(coalesce(
            (cast(d.mi_datetime as DATE) between
                date_add(date '{training_end_date}', INTERVAL {gap})
                and
                date_add(date_add(date '{training_end_date}', interval {gap}), interval {outcome_window})
            ), false
        ) as INT64) as y
    from
        mi_testwindow_elig_perc te
        left join mi_dates d on d.person_id = te.person_id
    where
        (
            d.mi_datetime is null
            or cast(d.mi_datetime as DATE) >= date_add(date '{training_end_date}', interval {gap})
        )

