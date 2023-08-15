INSERT INTO STV230530__DWH.h_accounts(hk_account_id, account_number, load_dt,load_src)
select
       distinct hash(account_number_from) as  hk_account_id,
       account_number_from,
       now() as load_dt,
       's3' as load_src
       from STV230530__STAGING.transactions
where hash(account_number_from) not in (select hk_account_id from STV230530__DWH.h_accounts);
