INSERT INTO STV230530__DWH.h_transactions(hk_transaction_id, transaction_id,transaction_dt,load_dt,load_src)
select
       hash(transaction_id) as  hk_transaction_id,
       operation_id as transaction_id,
       transaction_dt,
       now() as load_dt,
       's3' as load_src
       from STV230530__STAGING.transactions
where hash(transaction_id) not in (select hk_transaction_id from STV230530__DWH.h_transactions);
