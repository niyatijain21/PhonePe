CREATE TABLE edw_adhoc.attempted_and_success_txns AS
(

SELECT
    UPPER(sender_bank_id) AS sender_bank,
	transaction_flow_type,
    CASE
        WHEN HOUR(transaction_timestamp) BETWEEN 17 AND 20 THEN 'PEAK_HOUR' 
        ELSE 'BAU_HOUR' 
    END AS flag,
    transaction_date,
    HOUR(transaction_timestamp) AS hour_of_the_day,
    CASE
        WHEN MINUTE(transaction_timestamp) BETWEEN 0 AND 9 THEN 'A'
        WHEN MINUTE(transaction_timestamp) BETWEEN 10 AND 19 THEN 'B'
        WHEN MINUTE(transaction_timestamp) BETWEEN 20 AND 29 THEN 'C'
        WHEN MINUTE(transaction_timestamp) BETWEEN 30 AND 39 THEN 'D'
        WHEN MINUTE(transaction_timestamp) BETWEEN 40 AND 49 THEN 'E'
        WHEN MINUTE(transaction_timestamp) BETWEEN 50 AND 59 THEN 'F'
    END AS section_of_the_hour,    
    COUNT(transaction_id) AS total_attempted_transactions,
    COUNT(CASE WHEN state = 'COMPLETED' AND pmod(flags,2) = 0 THEN transaction_id END) AS total_successful_transactions
FROM
    edw.payment_detail
WHERE
    CONCAT(year,substring(CONCAT(0,month),-2)) >= '202109'
	AND CONCAT(year,substring(CONCAT(0,month),-2)) NOT IN ('202112','202204')
    AND HOUR(transaction_timestamp) BETWEEN 9 AND 21
    AND payment_instrument = 'ACCOUNT'
    AND multipay_transaction_rank = 1
    AND UPPER(sender_bank_id) IN ('SBIN','HDFC','BARB','UBIN','PUNB','CNRB','ICIC','BKID','UTIB','PYTM','KKBK','CBIN','IDIB','AIRP','IBKL','MAHB','KARB','ANDB','UCBA','IPOS','IOBA','INDB','FDRL','FINO','IDFB','BDBL','KVBL','YESB','PKGB')
GROUP BY
    UPPER(sender_bank_id),
	transaction_flow_type,
    CASE
        WHEN HOUR(transaction_timestamp) BETWEEN 17 AND 20 THEN 'PEAK_HOUR' 
        ELSE 'BAU_HOUR' 
    END,
    transaction_date,
    HOUR(transaction_timestamp),
    CASE
        WHEN MINUTE(transaction_timestamp) BETWEEN 0 AND 9 THEN 'A'
        WHEN MINUTE(transaction_timestamp) BETWEEN 10 AND 19 THEN 'B'
        WHEN MINUTE(transaction_timestamp) BETWEEN 20 AND 29 THEN 'C'
        WHEN MINUTE(transaction_timestamp) BETWEEN 30 AND 39 THEN 'D'
        WHEN MINUTE(transaction_timestamp) BETWEEN 40 AND 49 THEN 'E'
        WHEN MINUTE(transaction_timestamp) BETWEEN 50 AND 59 THEN 'F'
    END
	
);