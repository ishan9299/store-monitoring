insert into temp_active_store
select
    st.store_id,
    date(ss.timestamp_utc) as date,
    timediff(
        max(time(ss.timestamp_utc)), min(time(ss.timestamp_utc))) as uptime
from store_timezone st
inner join store_status ss on st.store_id = ss.store_id
inner join store_business_hours sbh on sbh.store_id = ss.store_id
where 
    time(ss.timestamp_utc) between sbh.start_time_local and sbh.end_time_local
    and weekday(ss.timestamp_utc) = sbh.day_of_week
    and ss.status = "active"
group by st.store_id, date(ss.timestamp_utc)
order by st.store_id;

insert into temp_inactive_store
select
    st.store_id,
    date(ss.timestamp_utc) as date,
    timediff(
        max(time(ss.timestamp_utc)), min(time(ss.timestamp_utc))) as downtime
from store_timezone st
inner join store_status ss on st.store_id = ss.store_id
inner join store_business_hours sbh on sbh.store_id = ss.store_id
where 
    time(ss.timestamp_utc) between sbh.start_time_local and sbh.end_time_local
    and weekday(ss.timestamp_utc) = sbh.day_of_week
    and ss.status = "inactive"
group by st.store_id, date(ss.timestamp_utc)
order by st.store_id;

/* active week */
SELECT
    store_id,
    SEC_TO_TIME(SUM(TIME_TO_SEC(uptime))) AS uptime_last_week
FROM temp_active_store
GROUP BY store_id;

/* inactive week */
SELECT
    store_id,
    SEC_TO_TIME(SUM(TIME_TO_SEC(uptime))) AS downtime_last_week
FROM temp_inactive_store
GROUP BY store_id;

select
    ss.store_id,
    SEC_TO_TIME(SUM(TIME_TO_SEC(tas.uptime))) AS uptime_last_week
    SEC_TO_TIME(SUM(TIME_TO_SEC(tis.uptime))) AS downtime_last_week
FROM store_status ss, temp_active_store tas, temp_inactive_store tis
GROUP BY ss.store_id;

SELECT
    a.store_id,
    a.uptime_last_week,
    i.downtime_last_week
FROM
    (SELECT
         store_id,
         SEC_TO_TIME(SUM(TIME_TO_SEC(uptime))) AS uptime_last_week
     FROM temp_active_store
     GROUP BY store_id) a
JOIN
    (SELECT
         store_id,
         SEC_TO_TIME(SUM(TIME_TO_SEC(uptime))) AS downtime_last_week
     FROM temp_inactive_store
     GROUP BY store_id) i
ON a.store_id = i.store_id;
