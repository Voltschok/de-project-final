INSERT INTO STV230530__DWH.h_transactions(hk_transaction_id,transaction_id,load_dt,load_src)
select
       distinct hash(operation_id) as  hk_transaction_id,
       operation_id as transaction_id,

       now() as load_dt,
       's3' as load_src
       from STV230530__STAGING.transactions
where hash(operation_id) not in (select hk_transaction_id from STV230530__DWH.h_transactions);
