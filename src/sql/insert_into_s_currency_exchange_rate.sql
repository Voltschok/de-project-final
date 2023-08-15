INSERT INTO STV230530__DWH.s_currency_exchange_rate(hk_currency_id  , currency_code_with , date_update,  currency_with_div , load_dt,load_src)
select hc.hk_currency_id ,
c.currency_code_with,
c.date_update datetime,
c.currency_with_div ,
now() as load_dt,
's3' as load_src
from STV230530__DWH.h_currencies as hc
left join STV230530__STAGING.currencies as c on hc.currency_code  = c.currency_code;
