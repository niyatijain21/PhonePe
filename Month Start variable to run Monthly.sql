Let vToday = MonthStart(Today())+1;
trace $(vToday);
If '$(vToday)' = Today() then

dummy_1:
SQL
set tez.queue.name = analytics;


NoConcatenate
raw_qvd_data_1:
SQL

select year_month as year_month_1,payment_type as payment_type_1,
transaction_context as transaction_context_1,
sender_vpa as sender_vpa_1,
payment_instrument as payment_instrument_1,
new_backenderrorcode as new_backenderrorcode_1,
cardissuer as cardissuer_1,
senderbank as senderbank_1,
purpose_code  as purpose_code_1,
case
when ((transaction_context = 'PEER_TO_PEER') AND (reciever_mcc='INTERNAL_USER' OR reciever_mcc = 'EXTERNAL_USER')) then 'P2P'
when ((payment_type = 'CONSUMER_TO_MERCHANT' or payment_type = 'CONSUMER_TO_MERCHANT_V2' OR payment_type = 'CONSUMER_TO_EXTERNAL_V2' OR payment_type = 'CONSUMER_TO_EXTERNAL') 
and reciever_mcc<> 'EXTERNAL_USER' AND reciever_mcc<> 'P2P_MERCHANT') then 'P2M'
when reciever_mcc='P2P_MERCHANT' then 'P2PM'
end as flow_type_1,

SUM(totalamount) AS totalamount_1,
SUM(totaltransactionid) AS totaltransactionid_1,
SUM(deemedtransactiontotal) AS deemedtransactiontotal_1,
SUM(tcctransactiontotal) AS tcctransactiontotal_1,
SUM(qco_enrollment_success) AS qco_enrollment_success_1
from edw_shared.payment_daily_txn_aggregate
where year_month>='202108'
GROUP BY

year_month,payment_type,
transaction_context,
sender_vpa,
payment_instrument,
new_backenderrorcode,
cardissuer,
senderbank,
purpose_code,
case
when ((transaction_context = 'PEER_TO_PEER') AND (reciever_mcc='INTERNAL_USER' OR reciever_mcc = 'EXTERNAL_USER')) then 'P2P'
when ((payment_type = 'CONSUMER_TO_MERCHANT' or payment_type = 'CONSUMER_TO_MERCHANT_V2' OR payment_type = 'CONSUMER_TO_EXTERNAL_V2' OR payment_type = 'CONSUMER_TO_EXTERNAL') 
and reciever_mcc<> 'EXTERNAL_USER' AND reciever_mcc<> 'P2P_MERCHANT') then 'P2M'
when reciever_mcc='P2P_MERCHANT' then 'P2PM'
end
;

STORE raw_qvd_data_1 into 'F:\NISHANK\success_dashboard_MONTHLY_qvd_lucy_1.qvd';

NoConcatenate
raw_qvdyearly_data:
SQL
select mon_year_2,flag_2,txns_2
from
(
select date_format(meta_date,'yyyyMM') as mon_year_2,Meta_Event as flag_2, sum(PB_CountTxn) AS txns_2
from edw.nm_meta_call_v2
where date_format(meta_date,'yyyyMM')>='202108' and Meta_Event = 'PAY_BAD_REQUEST' 
and pb_error in 
('UPI_TEMPORARY_DISABLED','TRANSFER_MODE_CURRENTLY_DISABLED','BANK_SENDER_ROLE_TEMPORARY_DISABLED',
'BANK_RECEIVER_ROLE_TEMPORARY_DISABLED','BANK_UPI_TEMPORARY_DISABLED','BANK_SENDER_PSP_TEMPORARY_DISABLED','BANK_RECEIVER_PSP_TEMPORARY_DISABLED')
group by date_format(meta_date,'yyyyMM'),Meta_Event

union all

SELECT year_month as mon_year_2, 'TOTAL' as flag_2, sum(Totaltransactionid)as txns_2
from edw_shared.payment_daily_txn_aggregate
where year_month >='202108' and Payment_instrument = 'UPI'
group by year_month, 'TOTAL'

union all

select date_format(mis_transaction_date,'yyyyMM') as ym,'ADJUSTED' as flag_2,sum(recover_user*tpc) as txn_2
from
(
SELECT mis_transaction_date,(mis_fulfilled_txns/ mis_num_customers) as tpc
FROM edw_shared.mis_txn_metrics
WHERE grouping_id=31 and mis_transaction_date>='2021-08-01'
)t1
left join
(
SELECT recovery_date,recovery_same_day as recover_user
FROM edw.mis_outage_recovery
WHERE recovery_date>='2021-08-01'
)t2
on t1.mis_transaction_date=t2.recovery_date
group by date_format(mis_transaction_date,'yyyyMM'),'ADJUSTED'
) a
;

STORE raw_qvdyearly_data into 'F:\NISHANK\success_dashboard_MONTHLY_qvd_lucy_2.qvd';

NoConcatenate
KS_monthly_data:

SQL


select substr(eventData_rulename,length(eventData_rulename)-3,4) as ano_bank_id_3, 
count(distinct case 

when substr(eventData_rulename,length(eventData_rulename)-3,4) = 'SBIN' and eventData_ruleId = 'R2004241145086832376942'
	and (cast (substring(regexp_replace(cast(eventData_predicateResult as string),'\\$','0'),locate('ybl', cast(eventData_predicateResult as string))+65,4) as string) = 'true' OR
cast (substring(regexp_replace(cast(eventData_predicateResult as string),'\\$','0'),locate('ybl', cast(eventData_predicateResult as string))+9,4) as string) = 'true') and (cast (substring(regexp_replace(cast(eventData_thresholdBreaches as string),'\\$','0'),locate('eventData.senderPsp=ybl', cast(eventData_thresholdBreaches as string))+63,4) as double)<45 OR
cast (substring(regexp_replace(cast(eventData_thresholdBreaches as string),'\\$','0'),locate('eventData.senderPsp=ybl', cast(eventData_thresholdBreaches as string))+34,4) as double)<45 ) then date_format(`time`,'dd HH:mm')
	
when substr(eventData_rulename,length(eventData_rulename)-3,4)in ('PUNB','UTIB','BARB','ICIC','HDFC') 
	and  eventData_ruleId in ('R2004241148042530126183','R2004241201051980311050','R2004241202280200311948','R2004241159093150126160','R2004241157041434253505')
	and (cast (substring(regexp_replace(cast(eventData_predicateResult as string),'\\$','0'),locate('ybl', cast(eventData_predicateResult as string))+65,4) as string) = 'true' OR
cast (substring(regexp_replace(cast(eventData_predicateResult as string),'\\$','0'),locate('ybl', cast(eventData_predicateResult as string))+9,4) as string) = 'true') and (cast (substring(regexp_replace(cast(eventData_thresholdBreaches as string),'\\$','0'),locate('eventData.senderPsp=ybl', cast(eventData_thresholdBreaches as string))+63,4) as double)<50 OR
cast (substring(regexp_replace(cast(eventData_thresholdBreaches as string),'\\$','0'),locate('eventData.senderPsp=ybl', cast(eventData_thresholdBreaches as string))+34,4) as double)<50 ) then date_format(`time`,'dd HH:mm')	

when substr(eventData_rulename,length(eventData_rulename)-3,4) in ('PUNB','UTIB','BARB','ICIC','HDFC','SBIN') 
	and eventData_ruleId in ('R2004241148575764253765','R2004241201428110311054','R2004241202537492376687','R2004241200074760311173','R2004241158070704253395','R2004241145530002376502')
	and (cast (substring(regexp_replace(cast(eventData_predicateResult as string),'\\$','0'),locate('ybl', cast(eventData_predicateResult as string))+65,4) as string) = 'true' OR
cast (substring(regexp_replace(cast(eventData_predicateResult as string),'\\$','0'),locate('ybl', cast(eventData_predicateResult as string))+9,4) as string) = 'true') and (cast (substring(regexp_replace(cast(eventData_thresholdBreaches as string),'\\$','0'),locate('eventData.senderPsp=ybl', cast(eventData_thresholdBreaches as string))+63,4) as double)<80 OR
cast (substring(regexp_replace(cast(eventData_thresholdBreaches as string),'\\$','0'),locate('eventData.senderPsp=ybl', cast(eventData_thresholdBreaches as string))+34,4) as double)<80 )  then date_format(`time`,'dd HH:mm')

end) as ano_count_3
	
from foxtrot_stream.anomaly_detection_default
where concat(`year`,substr(concat('00',`month`),-2)) = (date_format(date_add(current_date(),-1),'yyyyMM'))
and eventData_ruleId in ('R2004241148042530126183','R2004241157041434253505','R2004241159093150126160','R2004241201051980311050','R2004241145086832376942','R2004241202280200311948','R2004241148575764253765','R2004241158070704253395','R2004241200074760311173','R2004241201428110311054','R2004241145530002376502','R2004241202537492376687')
and eventData_thresholdBreaches is not null and eventData_thresholdBreaches <> '[]'
group by  substr(eventData_rulename,length(eventData_rulename)-3,4)
;

STORE KS_monthly_data into 'F:\NISHANK\success_dashboard_MONTHLY_qvd_lucy_3.qvd';

NoConcatenate
MPSP_monthly_data:

SQL

select campaignid as campaignid_4,year_mon as year_mon_4, users as users_4
from
(
select campaignid,date_format(date_sub(current_date,1),'yyyyMM') as year_mon, count(distinct b.user_id) as users
from
	(
	select distinct a.user_id, 
	(
	case when axl_psp=1 and ybl_psp=0 and ibl_psp=0 then '3.Users with only axl' 
	when axl_psp=0 and ybl_psp=1 and ibl_psp=0 then '1.Users with only ybl'
	 when axl_psp=0 and ybl_psp=0 and ibl_psp=1 then '2.Users with only ibl' 
	 when axl_psp=1 and ybl_psp=1 and ibl_psp=0 then '5.Users with axl & ybl' 
	 when axl_psp=1 and ybl_psp=0 and ibl_psp=1 then '6.Users with axl & ibl' 
	 when axl_psp=0 and ybl_psp=1 and ibl_psp=1 then '4.Users with ybl & ibl'
	 when axl_psp=1 and ybl_psp=1 and ibl_psp=1 then '7.Users with All PSP handles'
	 else 'K'
	 end
	 ) as campaignid 
	from 
		(
		select user_id ,
		max(case when psp ='axl' then 1 else 0 end) as axl_psp,
		 max(case when psp ='ybl' then 1 else 0 end) as ybl_psp, 
		 max(case when psp ='ibl' then 1 else 0 end) as ibl_psp 
		from payment.account_vpas 
		where concat(year,substr(concat('00',month),-2))<=date_format(date_sub(current_date,1),'yyyyMM') 
		group by user_id 
		) as a 
	)b
group by campaignid,date_format(date_sub(current_date,1),'yyyyMM') 

UNION all

select campaignid,date_format(date_sub(current_date,32),'yyyyMM') as year_mon, count(distinct b.user_id) as users
from
	(
	select distinct a.user_id, 
	(
	case when axl_psp=1 and ybl_psp=0 and ibl_psp=0 then '3.Users with only axl' 
	when axl_psp=0 and ybl_psp=1 and ibl_psp=0 then '1.Users with only ybl'
	 when axl_psp=0 and ybl_psp=0 and ibl_psp=1 then '2.Users with only ibl' 
	 when axl_psp=1 and ybl_psp=1 and ibl_psp=0 then '5.Users with axl & ybl' 
	 when axl_psp=1 and ybl_psp=0 and ibl_psp=1 then '6.Users with axl & ibl' 
	 when axl_psp=0 and ybl_psp=1 and ibl_psp=1 then '4.Users with ybl & ibl'
	 when axl_psp=1 and ybl_psp=1 and ibl_psp=1 then '7.Users with All PSP handles'
	 else 'K'
	 end
	 ) as campaignid 
	from 
		(
		select user_id ,
		max(case when psp ='axl' then 1 else 0 end) as axl_psp,
		 max(case when psp ='ybl' then 1 else 0 end) as ybl_psp, 
		 max(case when psp ='ibl' then 1 else 0 end) as ibl_psp 
		from payment.account_vpas 
		where concat(year,substr(concat('00',month),-2))<=date_format(date_sub(current_date,32),'yyyyMM') 
		group by user_id 
		) as a 
	)b
group by campaignid,date_format(date_sub(current_date,32),'yyyyMM') 

union all

select campaignid,date_format(date_sub(current_date,63),'yyyyMM') as year_mon, count(distinct b.user_id) as users
from
	(
	select distinct a.user_id, 
	(
	case when axl_psp=1 and ybl_psp=0 and ibl_psp=0 then '3.Users with only axl' 
	when axl_psp=0 and ybl_psp=1 and ibl_psp=0 then '1.Users with only ybl'
	 when axl_psp=0 and ybl_psp=0 and ibl_psp=1 then '2.Users with only ibl' 
	 when axl_psp=1 and ybl_psp=1 and ibl_psp=0 then '5.Users with axl & ybl' 
	 when axl_psp=1 and ybl_psp=0 and ibl_psp=1 then '6.Users with axl & ibl' 
	 when axl_psp=0 and ybl_psp=1 and ibl_psp=1 then '4.Users with ybl & ibl'
	 when axl_psp=1 and ybl_psp=1 and ibl_psp=1 then '7.Users with All PSP handles'
	 else 'K'
	 end
	 ) as campaignid 
	from 
		(
		select user_id ,
		max(case when psp ='axl' then 1 else 0 end) as axl_psp,
		 max(case when psp ='ybl' then 1 else 0 end) as ybl_psp, 
		 max(case when psp ='ibl' then 1 else 0 end) as ibl_psp 
		from payment.account_vpas 
		where concat(year,substr(concat('00',month),-2))<=date_format(date_sub(current_date,63),'yyyyMM') 
		group by user_id 
		) as a 
	)b
group by campaignid,date_format(date_sub(current_date,63),'yyyyMM') 

UNION all

select campaignid,date_format(date_sub(current_date,93),'yyyyMM') as year_mon, count(distinct b.user_id) as users
from
	(
	select distinct a.user_id, 
	(
	case when axl_psp=1 and ybl_psp=0 and ibl_psp=0 then '3.Users with only axl' 
	when axl_psp=0 and ybl_psp=1 and ibl_psp=0 then '1.Users with only ybl'
	 when axl_psp=0 and ybl_psp=0 and ibl_psp=1 then '2.Users with only ibl' 
	 when axl_psp=1 and ybl_psp=1 and ibl_psp=0 then '5.Users with axl & ybl' 
	 when axl_psp=1 and ybl_psp=0 and ibl_psp=1 then '6.Users with axl & ibl' 
	 when axl_psp=0 and ybl_psp=1 and ibl_psp=1 then '4.Users with ybl & ibl'
	 when axl_psp=1 and ybl_psp=1 and ibl_psp=1 then '7.Users with All PSP handles'
	 else 'K'
	 end
	 ) as campaignid 
	from 
		(
		select user_id ,
		max(case when psp ='axl' then 1 else 0 end) as axl_psp,
		 max(case when psp ='ybl' then 1 else 0 end) as ybl_psp, 
		 max(case when psp ='ibl' then 1 else 0 end) as ibl_psp 
		from payment.account_vpas 
		where concat(year,substr(concat('00',month),-2))<=date_format(date_sub(current_date,93),'yyyyMM') 
		group by user_id 
		) as a 
	)b
group by campaignid,date_format(date_sub(current_date,93),'yyyyMM') 

Union all

select campaignid,date_format(date_sub(current_date,124),'yyyyMM') as year_mon, count(distinct b.user_id) as users
from
	(
	select distinct a.user_id, 
	(
	case when axl_psp=1 and ybl_psp=0 and ibl_psp=0 then '3.Users with only axl' 
	when axl_psp=0 and ybl_psp=1 and ibl_psp=0 then '1.Users with only ybl'
	 when axl_psp=0 and ybl_psp=0 and ibl_psp=1 then '2.Users with only ibl' 
	 when axl_psp=1 and ybl_psp=1 and ibl_psp=0 then '5.Users with axl & ybl' 
	 when axl_psp=1 and ybl_psp=0 and ibl_psp=1 then '6.Users with axl & ibl' 
	 when axl_psp=0 and ybl_psp=1 and ibl_psp=1 then '4.Users with ybl & ibl'
	 when axl_psp=1 and ybl_psp=1 and ibl_psp=1 then '7.Users with All PSP handles'
	 else 'K'
	 end
	 ) as campaignid 
	from 
		(
		select user_id ,
		max(case when psp ='axl' then 1 else 0 end) as axl_psp,
		 max(case when psp ='ybl' then 1 else 0 end) as ybl_psp, 
		 max(case when psp ='ibl' then 1 else 0 end) as ibl_psp 
		from payment.account_vpas 
		where concat(year,substr(concat('00',month),-2))<=date_format(date_sub(current_date,124),'yyyyMM') 
		group by user_id 
		) as a 
	)b
group by campaignid,date_format(date_sub(current_date,124),'yyyyMM') 
)t1;

STORE MPSP_monthly_data into 'F:\NISHANK\success_dashboard_MONTHLY_qvd_lucy_4.qvd';

else

NoConcatenate
raw_qvd_data_1:
LOAD *
FROM [F:\NISHANK\success_dashboard_MONTHLY_qvd_lucy_1.qvd] (qvd); 

NoConcatenate
raw_qvdyearly_data:
LOAD *
FROM [F:\NISHANK\success_dashboard_MONTHLY_qvd_lucy_2.qvd] (qvd); 

NoConcatenate
KS_monthly_data:
LOAD *
FROM [F:\NISHANK\success_dashboard_MONTHLY_qvd_lucy_3.qvd] (qvd); 

NoConcatenate
MPSP_monthly_data:
LOAD *
FROM [F:\NISHANK\success_dashboard_MONTHLY_qvd_lucy_4.qvd] (qvd); 

ENDIF
