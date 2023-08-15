INSERT INTO STV230530__DWH.h_currencies(date_update, hk_currency_id, currency_code, currency_code_with,load_dt,load_src)
select
       date_update,
       hash(currency_code, currency_code_with, date_update) as  hk_currency_id,
       currency_code,
       currency_code_with,
       
       now() as load_dt,
       's3' as load_src
       from STV230530__STAGING.currencies
where hash(currency_code, currency_code_with, date_update) not in (select hk_currency_id from STV230530__DWH.h_currencies);
