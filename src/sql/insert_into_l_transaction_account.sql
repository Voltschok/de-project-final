INSERT INTO STV230530__DWH.l_transaction_account(hk_l_transaction_account, hk_transaction_id, hk_account_id,load_dt,load_src)
select
hash(ht.hk_transaction_id, ha.hk_account_id) as hk_l_transaction_account,
ht.hk_transaction_id,
ha.hk_account_id,
now() as load_dt,
's3' as load_src
from STV230530__STAGING.transactions  as t
left join STV230530__DWH.h_transactions  as ht on t.operation_id = ht.transaction_id
left join STV230530__DWH.h_accounts  as ha on t.account_number_from = ha.account_number
where hash(ht.hk_transaction_id, ha.hk_account_id) not in (select hk_l_transaction_account from STV230530__DWH.l_transaction_account);
