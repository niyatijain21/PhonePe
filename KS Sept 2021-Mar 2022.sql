%jdbc(hive)
SET tez.queue.name=analytics;
SELECT 
	UPPER(eventdata_exceptioncontext_providerid) AS sender_bank,
	eventdata_newworkflowtype as flow,
	CASE
        WHEN HOUR(`time`) BETWEEN 17 AND 20 THEN 'PEAK_HOUR' 
        ELSE 'BAU_HOUR' 
    END AS flag,
	DATE(`time`) AS transaction_date,
	HOUR(`time`) AS hour_of_the_day,
	CASE 
		WHEN MINUTE(`time`)>=0 AND MINUTE(`time`)<=9 THEN 'A.1-9'
		WHEN MINUTE(`time`)>=10 AND MINUTE(`time`)<=19 THEN 'B.10-19'
		WHEN MINUTE(`time`)>=20 AND MINUTE(`time`)<=29 THEN 'C.20-29'
		WHEN MINUTE(`time`)>=30 AND MINUTE(`time`)<=39 THEN 'D.30-39'
		WHEN MINUTE(`time`)>=40 AND MINUTE(`time`)<=49 THEN 'E.40-49'
		WHEN MINUTE(`time`)>=50 AND MINUTE(`time`)<=59 THEN 'F.50-59'
	END AS section_of_the_hour,
	COUNT(DISTINCT eventdata_transactionid) AS kill_switch_txns
FROM 
	 foxtrot_stream.payment_audit
	WHERE 
		eventtype = 'PAY_BAD_REQUEST' 
		AND eventdata_errorcode IN ('BANK_SENDER_ROLE_TEMPORARY_DISABLED', 
		'BANK_RECEIVER_ROLE_TEMPORARY_DISABLED', 'UPI_TEMPORARY_DISABLED', 
		'BANK_UPI_TEMPORARY_DISABLED','TRANSFER_MODE_CURRENTLY_DISABLED',
		'BANK_SENDER_PSP_TEMPORARY_DISABLED','BANK_RECEIVER_PSP_TEMPORARY_DISABLED') 
		AND year=2021 AND month=09
		concat(year,substr(concat('00',month),-2), substr(concat('00', day),-2)) between 20210901 and 20210915
GROUP BY 
	UPPER(eventdata_exceptioncontext_providerid),
	eventdata_newworkflowtype,
	CASE
        WHEN HOUR(`time`) BETWEEN 17 AND 20 THEN 'PEAK_HOUR' 
        ELSE 'BAU_HOUR' 
    END,
	DATE(`time`),
	HOUR(`time`),
	CASE 
		WHEN MINUTE(`time`)>=0 AND MINUTE(`time`)<=9 THEN 'A.1-9'
		WHEN MINUTE(`time`)>=10 AND MINUTE(`time`)<=19 THEN 'B.10-19'
		WHEN MINUTE(`time`)>=20 AND MINUTE(`time`)<=29 THEN 'C.20-29'
		WHEN MINUTE(`time`)>=30 AND MINUTE(`time`)<=39 THEN 'D.30-39'
		WHEN MINUTE(`time`)>=40 AND MINUTE(`time`)<=49 THEN 'E.40-49'
		WHEN MINUTE(`time`)>=50 AND MINUTE(`time`)<=59 THEN 'F.50-59'
	END