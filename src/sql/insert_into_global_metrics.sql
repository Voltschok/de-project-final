SELECT now()::data as date_update, currency, 
  count(distinct transactions),
  avg(), 
FROM h_transactions ht
LEFT JOIN h_currencies hs on ht.
