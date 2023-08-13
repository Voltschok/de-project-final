INSERT INTO STV230530__DWH.h_transactions(hk_transaction_id, transaction_id,transaction_dt,load_dt,load_src)
select
       hash(transaction_id) as  hk_transaction_id,
       operation_id as transaction_id,
       transaction_dt,
       now() as load_dt,
       's3' as load_src
       from STV230530__STAGING.transactions
where hash(transaction_id) not in (select hk_transaction_id from STV230530__DWH.h_transactions);

INSERT INTO STV230530__DWH.h_currencies(hk_currency_id, currency_code,date_update,load_dt,load_src)
select
       hash(currency_code) as  hk_currency_id,
       currency_code,
       date_update,
       now() as load_dt,
       's3' as load_src
       from STV230530__STAGING.currencies
where hash(currency_code) not in (select hk_currency_id from STV230530__DWH.h_currencies);

INSERT INTO STV230530__DWH.h_accounts(hk_account_id, account_number, load_dt,load_src)
select
       hash(currency_code) as  hk_account_id,
       account_number_from,
       now() as load_dt,
       's3' as load_src
       from STV230530__STAGING.transactions
where hash(account_number_from) not in (select hk_account_id from STV230530__DWH.h_accounts);
