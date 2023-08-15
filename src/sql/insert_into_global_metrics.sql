INSERT INTO STV230530__DWH.global_metrics (date_update, 
                                          currency_from, 
                                          amount_total, 
                                          cnt_transactions, 
                                          avg_transactions_per_account, 
                                          cnt_accounts_make_transactions)

WITH temp_transaction AS(  
SELECT  
  ht.hk_transaction_id, 
  ht.transaction_td,
  hc.currency_code as currency_from,
  scer.currency_with_div, 
  CASE WHEN  hc.currency_code=420 THEN sta.amount
  ELSE sta.amount*scer.currency_with_div  END AS amount 
  sts.status,
  lta.hk_account_id,
  ha.account_number_from
  
FROM STV230530__DWH.h_transactions ht
LEFT JOIN STV230530__DWH.l_transaction_currency ltc on ht.hk_transaction_id=ltc.hk_transaction_id
LEFT JOIN STV230530__DWH.h_currencies hc on hc.hk_currency_id=ltc.hk_currency_id
LEFT JOIN STV230530__DWH.s_currency_exchange_rate scer on scer.hk_currency_id = hc.hk_currency_id
LEFT JOIN STV230530__DWH.s_transactions_amount sta on sta.hk_transaction_id = ht.hk_transaction_id
LEFT JOIN STV230530__DWH.l_transaction_account lta on ht.hk_transaction_id=lta.hk_transaction_id
LEFT JOIN STV230530__DWH.h_accounts  as ha on lta.hk_account_id = ha.hk_account_id
LEFT JOIN STV230530__DWH.s_transactions_status sts on sts.hk_transaction_id = ht.hk_transaction_id
LEFT JOIN STV230530__DWH.s_transactions_type stt on stt.hk_transaction_id = ht.hk_transaction_id
  
WHERE ht.transaction_td::DATE = '{{ ds }}'-1
AND sts.status = 'done'
AND stt."type" != 'authorisation'  
AND ha.account_number_from>0
) 

  SELECT '{{ ds }}', --now()::data as date_update, 
      currency_from,
      sum(amount) as amount_total,
      count(distinct hk_transaction_id),
      count(distinct hk_transaction_id)/count(distinct hk_account_id) as avg_transactions_per_account,  
      count(distinct hk_account_id) as cnt_accounts_make_transactions
FROM temp_table
GROUP BY date_update, currency_from


-- date_update — дата расчёта,
-- currency_from — код валюты транзакции;
-- amount_total — общая сумма транзакций по валюте в долларах;
-- cnt_transactions — общий объём транзакций по валюте;
-- avg_transactions_per_account — средний объём транзакций с аккаунта;
-- cnt_accounts_make_transactions — количество уникальных аккаунтов с совершёнными транзакциями по валюте.
