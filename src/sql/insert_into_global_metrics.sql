INSERT INTO STV230530__DWH.global_metrics (date_update, 
                                          currency_from, 
                                          amount_total, 
                                          cnt_transactions, 
                                          avg_transactions_per_account, 
                                          cnt_accounts_make_transactions)

WITH temp_transaction AS(  
SELECT  
  ht.hk_transaction_id, 
  hc.currency_code as currency from, 
  sta.amount 
  
FROM STV230530__DWH.h_transactions ht
LEFT JOIN STV230530__DWH.l_transaction_currency ltc on ht.hk_transaction_id=ltc.hk_transaction_id
LEFT JOIN STV230530__DWH.h_currencies hc on hc.hk_currency_id=ltc.hk_currency_id
LEFT JOIN STV230530__DWH.s_transactions_amount sta on sta.hk_transaction_id = ht.hk_transaction_id
WHERE ht.transaction_td = '{{ ds }}'
),
  
temp_currency AS(
  SELECT  
  ht.transaction_id, 
  hc.currency_code as currency_from, 
  sum(sta.amount) as amount_total
FROM STV230530__DWH.h_currencies hc on hc.hk_currency_id=ltc.hk_currency_id
WHERE hc.date_update = '{{ ds }}'
  )

 
  
   



SELECT '{{ ds }}', --now()::data as date_update, 
      currency_from,
      sum(amount) as amount_total,
      count(distinct hk_transaction_id),
      count(distinct transactions)/count(distinct hk_account_id) as avg_transactions_per_account,  
      count(distinct hk_account_id) as cnt_accounts_make_transactions
FROM temp_table
GROUP BY date_update, currency_from


-- date_update — дата расчёта,
-- currency_from — код валюты транзакции;
-- amount_total — общая сумма транзакций по валюте в долларах;
-- cnt_transactions — общий объём транзакций по валюте;
-- avg_transactions_per_account — средний объём транзакций с аккаунта;
-- cnt_accounts_make_transactions — количество уникальных аккаунтов с совершёнными транзакциями по валюте.
