
INSERT INTO STV230530__DWH.l_transaction_currency(hk_l_transaction_currency, 
													hk_transaction_id, 
													hk_currency_id,
													load_dt,
													load_src)
select
hash(ht.hk_transaction_id, hc.hk_currency_id),
ht.hk_transaction_id,
hc.hk_currency_id,
now() as load_dt,
's3' as load_src
from STV230530__STAGING.transactions  as t
left join STV230530__DWH.h_currencies  as hc on t.currency_code = hc.currency_code
left join STV230530__DWH.h_transactions as ht on t.operation_id = ht.transaction_id
where hash(ht.hk_transaction_id, hc.hk_currency_id) not in (select hk_l_transaction_currency from STV230530__DWH.l_transaction_currency);
