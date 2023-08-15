INSERT INTO STV230530__DWH.l_transaction_currency(hk_l_transaction_currency, 
													hk_transaction_id, 
													hk_currency_id,
													load_dt,
													load_src)
select
distinct hash(ht.hk_transaction_id, hc.hk_currency_id) as hk_l_transaction_currency ,

ht.hk_transaction_id,
hc.hk_currency_id,
now() as load_dt,
's3' as load_src
from STV230530__DWH.h_transactions as ht 
left join  STV230530__STAGING.transactions as t on t.operation_id = ht.transaction_id --and t.transaction_dt=hc.date_update 
left join STV230530__DWH.h_currencies as hc  on t.currency_code = hc.currency_code
where hash(ht.hk_transaction_id, hc.hk_currency_id) not in (select hk_l_transaction_currency from STV230530__DWH.l_transaction_currency);
