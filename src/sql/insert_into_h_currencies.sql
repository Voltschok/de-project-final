INSERT INTO STV230530__DWH.h_currencies(hk_currency_id, currency_code, load_dt,load_src)
select
        
       distinct hash(currency_code) as  hk_currency_id,
       currency_code,
 
       
       now() as load_dt,
       's3' as load_src
       from STV230530__STAGING.currencies
where hash(currency_code ) not in (select hk_currency_id from STV230530__DWH.h_currencies);
