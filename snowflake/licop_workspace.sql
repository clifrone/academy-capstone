select country, entity, location,sensortype,parameter, count(1), avg(value), sum(value) from raw_open_aq_data
group by ALL
order by 4 desc

commit

drop table raw_open_aq_data


create table load_executions (exec_date date)

select * from  load_executions


select 'simple' as tbl_n, count(1) as cnt from raw_open_aq_data
union all 
select 'append' , count(1) from raw_open_aq_data_appended;


select * from raw_open_aq_data;

select country, entity, location,sensortype,parameter, min(utc_date), max(utc_date), count(1), avg(value), sum(value) from raw_open_aq_data
group by ALL
order by 4 desc;

//Show the monthly maximum values of each air quality metric
select  
    location,
    year(utc_date::TIMESTAMP) ||'/'|| lpad(month(utc_date::TIMESTAMP),2,0) year_month,
    parameter,  
    count(1), 
    avg(value), 
    sum(value) 
from 
    raw_open_aq_data
group by ALL
order by 1,2,3;

//Compare the daily max of each metric to its 7-day historic maximum. You will require window functions
with 
daily_max as 
(
    select  
        location,
        year(utc_date::TIMESTAMP) ||'/'|| lpad(month(utc_date::TIMESTAMP),2,0)||'/'|| lpad(day(utc_date::TIMESTAMP),2,0) year_month_day,
        parameter,  
        count(1), 
        max(value) daily_max_value
    from 
        raw_open_aq_data
    group by ALL
    order by 1,2,3
),
 daily_max_month as 
(
    select 
        location, 
        parameter, 
        year_month_day,
        daily_max_value,
        max(daily_max_value) over (partition by location, parameter order by year_month_day rows between 6 preceding and current row) last_6_days_max
    from daily_max
    
)
select * from daily_max_month
order by location, parameter, year_month_day;
    
