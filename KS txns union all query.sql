%jdbc(hive)
SET tez.queue.name=analytics;
SELECT
	sender_bank,
	flow,
	flag,
	transaction_date,
	hour_of_the_day,
	section_of_the_hour,
	kill_switch_txns
FROM
	(
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
			AND year=2021 AND month=09 AND
			concat(year,substr(concat('00',month),-2)) =202109
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
		
	UNION ALL

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
			AND year=2021 AND month=09 AND
			concat(year,substr(concat('00',month),-2)) =202110
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
		
	UNION ALL
	
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
			AND year=2021 AND month=09 AND
			concat(year,substr(concat('00',month),-2)) =202111
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
		
		UNION ALL
		
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
				AND year=2021 AND month=09 AND
				concat(year,substr(concat('00',month),-2)) =202201
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
			
		UNION ALL
		
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
				AND year=2021 AND month=09 AND
				concat(year,substr(concat('00',month),-2)) =202202
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
		
		UNION ALL
		
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
				AND year=2021 AND month=09 AND
				concat(year,substr(concat('00',month),-2), substr(concat('00', day),-2)) between '20220301' and '20220310'
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
		
		UNION ALL
		
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
				AND year=2021 AND month=09 AND
				concat(year,substr(concat('00',month),-2), substr(concat('00', day),-2)) between '20220311' and '20220320'
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
		
		UNION ALL
		
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
				AND year=2021 AND month=09 AND
				concat(year,substr(concat('00',month),-2), substr(concat('00', day),-2)) between '20220321' and '20220330'
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
) as a