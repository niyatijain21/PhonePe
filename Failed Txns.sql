%jdbc(hive)
set tez.queue.name=analytics;
select
    date(transaction_timestamp),
    hour(transaction_timestamp),
    count(distinct transaction_id)
from
    edw.payment_detail
where
    concat(year,substr(concat('00',month),-2)) >= 202201
	AND month in (1,2,3)
    AND (payment_category IS NULL 
    OR state != 'COMPLETED' 
    OR pmod(flags,2) != 0 
    OR (nexus_state IS NOT NULL AND nexus_state != 'COMPLETED'))
group by
    date(transaction_timestamp),
    hour(transaction_timestamp)