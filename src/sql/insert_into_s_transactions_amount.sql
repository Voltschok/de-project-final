INSERT INTO STV230530__DWH.s_transactions_amount(hk_transaction_id , amount, load_dt,load_src)
select ht.hk_transaction_id,
t.amount,
now() as load_dt,
's3' as load_src
from STV230530__DWH.h_transactions as ht
left join STV230530__STAGING.transactions as t on ht.transaction_id  = t.operation_id;
