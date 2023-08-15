INSERT INTO STV230530__DWH.global_metrics (date_update, 
                                          currency_from, 
                                          amount_total, 
                                          cnt_transactions, 
                                          avg_transactions_per_account, 
                                          cnt_accounts_make_transactions)
SELECT 
  now()::data as date_update, currency, 
  hc.currency_code as currency_from,
  sum() as amount_total,
  count(distinct transactions),
  avg() as avg_transactions_per_account,  
  count() as cnt_accounts_make_transactions
  
FROM STV230530__DWH.h_transactions ht
LEFT JOIN STV230530__DWH.l_transaction_currency ltc on ht.hk_transaction_id=ltc.hk_transaction_id
LEFT JOIN STV230530__DWH.h_currencies hc on hc.hk_currency_id=ltc.hk_currency_id

  
SELECT now()::data as date_update, 
  ht.transaction_id, 
  hc.currency_code as currency_from, 
  sum(sta.amount) as amount_total
  
FROM STV230530__DWH.h_transactions ht
LEFT JOIN STV230530__DWH.s_transactions_amount sta on sta.hk_transaction_id = ht.hk_transaction_id
LEFT JOIN STV230530__DWH.l_transaction_currency ltc on ht.hk_transaction_id=ltc.hk_transaction_id
LEFT JOIN STV230530__DWH.h_currencies hc on hc.hk_currency_id=ltc.hk_currency_id
GROUP BY  date_update, ht.transaction_id

SELECT now()::data as date_update, 
  ht.transaction_id, 
  hc.currency_code as currency_from, 
  sum(sta.amount) as amount_total
  
FROM STV230530__DWH.h_transactions ht
GROUP BY  date_update, ht.transaction_id

SELECT now()::data as date_update, 
       currency_from,
      sum() as amount_total,
      count(distinct transactions),
      count(distinct transactions)( as avg_transactions_per_account,  
      count() as cnt_accounts_make_transactions

-- date_update — дата расчёта,
-- currency_from — код валюты транзакции;
-- amount_total — общая сумма транзакций по валюте в долларах;
-- cnt_transactions — общий объём транзакций по валюте;
-- avg_transactions_per_account — средний объём транзакций с аккаунта;
-- cnt_accounts_make_transactions — количество уникальных аккаунтов с совершёнными транзакциями по валюте.
