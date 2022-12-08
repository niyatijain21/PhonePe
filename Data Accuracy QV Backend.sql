ODBC CONNECT TO QV_Lucy3_Zookeeper;
SQL
set tez.queue.name=analytics;

UNQUALIFY *;
NoConcatenate
main_table:
SQL
SELECT 
   dt,
   metric_value,
   data_type,
   source
FROM 
   edw_shared.mis_data_accuracy_table
WHERE
	DATE(dt)>DATE_SUB(CURRENT_DATE(),3) AND DATE(dt)<=DATE_SUB(CURRENT_DATE(),1);

Concatenate
LOAD 
*
FROM [F:\QlikTech\Documents\QVD\MIS_Data_Accuracy.qvd] (qvd)
WHERE DATE(dt,'YYYY-MM-DD')<=DATE(TODAY()-3);

store main_table into F:\QlikTech\Documents\QVD\MIS_Data_Accuracy.qvd;

     
