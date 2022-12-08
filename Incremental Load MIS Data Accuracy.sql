DROP TABLE IF EXISTS edw_adhoc.mis_data_accuracy_metrics_delta
;
CREATE TABLE edw_adhoc.mis_data_accuracy_metrics_delta AS
(
SELECT
	dt,
	source,
	data_type,
	metric_value
FROM
	(
	SELECT
		DATE_FORMAT(DATE(`time`),'YYYY-MM-dd') AS dt,
		'foxtrot' AS source,
		'txns' AS data_type,
		COUNT(DISTINCT eventdata_transactionid) AS metric_value
	FROM											
		foxtrot_stream.payment_backend_transact
		WHERE											
		eventtype='PAYMENT'										
		AND eventdata_backenderrorcode='SUCCESS'										
		AND eventdata_workflowtype IN ('CONSUMER_TO_CONSUMER','CONSUMER_TO_MERCHANT','CONSUMER_TO_EXTERNAL','EXTERNAL_TO_MERCHANT','WALLET_TOPUP')										
		AND DATE(cast((concat(`year`,'-',substr(concat('00',`month`),-2),'-',substr(concat('00',`day`),-2)) ) AS DATE)) >= date_sub('2022-04-21',2)							
	GROUP BY											
		DATE_FORMAT(DATE(`time`),'YYYY-MM-dd')

	UNION ALL 

	SELECT
		DATE_FORMAT(DATE(`time`),'YYYY-MM-dd') AS dt,
		'foxtrot' AS source,
		'tpv' AS data_type,
		SUM(eventdata_totaltransactionamount)/100 AS metric_value
	FROM											
		foxtrot_stream.payment_backend_transact
	WHERE											
		eventtype='PAYMENT'										
		AND eventdata_backenderrorcode='SUCCESS'										
		AND eventdata_workflowtype IN ('CONSUMER_TO_CONSUMER','CONSUMER_TO_MERCHANT','CONSUMER_TO_EXTERNAL','EXTERNAL_TO_MERCHANT','WALLET_TOPUP')										
		AND DATE(cast((concat(`year`,'-',substr(concat('00',`month`),-2),'-',substr(concat('00',`day`),-2)) ) AS DATE)) >= date_sub('2022-04-21',2)	
	GROUP BY											
		DATE_FORMAT(DATE(`time`),'YYYY-MM-dd')
		
	UNION ALL 
	
	SELECT											
		DATE_FORMAT(DATE(created),'YYYY-MM-dd') AS dt,
		'payment' AS source,
		'txns' AS data_type,									
		COUNT(DISTINCT transaction_id) AS metric_value										
	FROM 
		payment.transactions
	WHERE 
		 backend_error_code='SUCCESS'								
		 AND flow IN ('CONSUMER_TO_CONSUMER', 'CONSUMER_TO_EXTERNAL', 'CONSUMER_TO_MERCHANT', 'WALLET_TOPUP', 'CONSUMER_TO_CONSUMER_V2',
		  'CONSUMER_TO_EXTERNAL_V2', 'CONSUMER_TO_MERCHANT_V2', 'WALLET_TOPUP_V2', 'EXTERNAL_TO_MERCHANT')
		 AND date(created) >=(date_sub('2022-04-21',2))
		 AND CONCAT(year, SUBSTRING(CONCAT(0,month),-2)) >=DATE_FORMAT(date_sub('2022-04-21',2),'yyyyMM')
	GROUP BY											
		DATE_FORMAT(DATE(created),'YYYY-MM-dd')

	UNION ALL

	SELECT											
		DATE_FORMAT(DATE(created),'YYYY-MM-dd') AS dt,
		'payment' AS source,
		'tpv' AS data_type,									
		SUM(amount)/100  AS metric_value
	FROM 
		payment.transactions
	WHERE 
		backend_error_code='SUCCESS'								
		AND flow IN ('CONSUMER_TO_CONSUMER', 'CONSUMER_TO_EXTERNAL', 'CONSUMER_TO_MERCHANT', 'WALLET_TOPUP', 'CONSUMER_TO_CONSUMER_V2',
		'CONSUMER_TO_EXTERNAL_V2', 'CONSUMER_TO_MERCHANT_V2', 'WALLET_TOPUP_V2', 'EXTERNAL_TO_MERCHANT')
		AND date(created) >=(date_sub('2022-04-21',2))
		AND CONCAT(year, SUBSTRING(CONCAT(0,month),-2)) >= DATE_FORMAT(date_sub('2022-04-21',2),'yyyyMM')
	GROUP BY											
		DATE_FORMAT(DATE(created),'YYYY-MM-dd')

	UNION ALL    
	
	SELECT
		DATE_FORMAT(DATE(transaction_date),'YYYY-MM-dd') AS dt,
		'payment_detail' AS source,
		'txns' AS data_type,	
		COUNT(DISTINCT transaction_id) AS metric_value
	from 
		edw.payment_detail
	WHERE 
		transaction_date >=(date_sub('2022-04-21',2))
		AND(state = 'COMPLETED' AND pmod(flags,2) = 0 AND (nexus_state IS NULL OR nexus_state = 'COMPLETED')) 
		AND CONCAT(year, SUBSTRING(CONCAT(0,month),-2)) >=DATE_FORMAT(date_sub('2022-04-21',2),'yyyyMM')
	GROUP BY											
		DATE_FORMAT(DATE(transaction_date),'YYYY-MM-dd')

	UNION ALL    

	SELECT
		DATE_FORMAT(DATE(transaction_date),'YYYY-MM-dd') AS dt,
		'payment_detail' AS source,
		'tpv' AS data_type,
		SUM(transaction_amount) AS metric_value
	from 
		edw.payment_detail
	WHERE 
		 DATE(transaction_date) >=(date_sub('2022-04-21',2))
		 AND(state = 'COMPLETED' AND pmod(flags,2) = 0 AND (nexus_state IS NULL OR nexus_state = 'COMPLETED')) 
		 AND CONCAT(year, SUBSTRING(CONCAT(0,month),-2)) >= DATE_FORMAT(DATE_SUB('2022-04-21',2),'yyyyMM')

	GROUP BY											
		DATE_FORMAT(DATE(transaction_date),'YYYY-MM-dd')

	UNION ALL

	SELECT											
		DATE_FORMAT(DATE(`time`),'YYYY-MM-dd') AS dt,
		'foxtrot' AS source,
		'customers'	AS data_type,									
		COUNT(DISTINCT eventdata_senderuser) AS metric_value

	FROM											
		foxtrot_stream.payment_backend_transact										
	WHERE											
		eventtype='PAYMENT'										
		AND eventdata_backenderrorcode='SUCCESS'										
		AND eventdata_workflowtype IN ('CONSUMER_TO_CONSUMER','CONSUMER_TO_MERCHANT','CONSUMER_TO_EXTERNAL','EXTERNAL_TO_MERCHANT','WALLET_TOPUP')										
		AND DATE(cast((concat(`year`,'-',substr(concat('00',`month`),-2),'-',substr(concat('00',`day`),-2)) ) AS DATE)) >= date_sub('2022-04-21',2)	
	GROUP BY											
		DATE_FORMAT(DATE(`time`),'YYYY-MM-dd')

	UNION ALL

	SELECT
		DATE_FORMAT(DATE(`time`),'YYYY-MM-dd')  AS dt,
		'foxtrot' AS source,
		'users' AS data_type,
		COUNT(DISTINCT eventdata_userid) AS metric_value
	FROM
		foxtrot_stream.phonepe_consumer_app_android_app_loaded_default
	WHERE
		eventtype = 'APP_LOADED'
		AND DATE(cast((concat(`year`,'-',substr(concat('00',`month`),-2),'-',substr(concat('00',`day`),-2)) ) AS DATE)) >= date_sub('2022-04-21',2)	
	GROUP BY 
		DATE_FORMAT(DATE(`time`),'YYYY-MM-dd')
		
	UNION ALL
	
	SELECT
		DATE_FORMAT(DATE(`time`),'YYYY-MM-dd')  AS dt,
		'foxtrot' AS source,
		'registrations' AS data_type,
		COUNT(DISTINCT eventdata_userid) AS metric_value
	FROM
		foxtrot_stream.userservice_user_intent
	WHERE
		eventtype = 'ACCEPT_TNC'
		AND DATE(cast((concat(`year`,'-',substr(concat('00',`month`),-2),'-',substr(concat('00',`day`),-2)) ) AS DATE)) >= date_sub('2022-04-21',2)	
	GROUP BY 
		DATE_FORMAT(DATE(`time`),'YYYY-MM-dd')

	) main 
	
)
;
DELETE FROM 
    edw_shared.mis_data_accuracy_table
WHERE 
    DATE(dt) >= DATE(date_sub('2022-04-21',2))
	
;	

INSERT INTO TABLE edw_shared.mis_data_accuracy_table
SELECT 
	dt,
	source,
	data_type,
	metric_value
FROM 
    edw_adhoc.mis_data_accuracy_metrics_delta
;
    