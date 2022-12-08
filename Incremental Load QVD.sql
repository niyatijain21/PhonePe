SQL
SET hive.execution.engine=tez;
new_dau_day_bucket_conversion:
NoConcatenate
SQL
SELECT
	DATE(registration_date) AS dt,
	COUNT(DISTINCT CASE WHEN DATE(registration_date)=DATE(first_txn_date) THEN user_id END) AS D0_customers,
	COUNT(DISTINCT CASE WHEN DATE(registration_date)>DATE_SUB(DATE(first_txn_date),4) AND DATE(registration_date)<=DATE(first_txn_date) THEN user_id END) AS D3_customers,
	COUNT(DISTINCT CASE WHEN DATE(registration_date)>DATE_SUB(DATE(first_txn_date),8) AND DATE(registration_date)<=DATE(first_txn_date) THEN user_id END) AS D7_customers
FROM 
	edw.user_dimension	
WHERE
	DATE(registration_date)>DATE_SUB(CURRENT_DATE(),8) AND DATE(registration_date)<=DATE_SUB(CURRENT_DATE(),1)
GROUP BY 
	DATE(registration_date);
	
Concatenate
LOAD 
*
FROM [F:\QlikTech\Documents\QVD\MIS_new_dau_day_bucket_conversion.qvd] (qvd)
WHERE DATE(dt,'YYYY-MM-DD')<=DATE(TODAY()-8);





