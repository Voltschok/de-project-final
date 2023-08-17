MERGE INTO STV230530__DWH.global_metrics AS gm
USING
    (WITH all_currency_transactions AS (
    SELECT t.operation_id,
        t.currency_code,
        t.account_number_from,
        t.amount,
        t.transaction_dt,
        t.status,
        t.transaction_type 
    FROM STV230530__STAGING.transactions AS t
    WHERE
        t.status='done' AND
        t.account_number_from>0 AND
        t.transaction_dt::date='{count_date}'::date-1 AND
        t.transaction_type !='authorisation'),

    dollar_currency_transactions AS (
    SELECT act.operation_id,
        act.currency_code as currency_from,
        act.account_number_from,
        act.transaction_dt,
        act.amount,
        act.status
    FROM all_currency_transactions AS act
    WHERE act.currency_code=420),

    non_dollar_currency_transactions AS (
    SELECT act.operation_id,
        act.currency_code as currency_from,
        act.account_number_from,
        act.transaction_dt,
        (act.amount*c.currency_with_div) as amount,
        act.status
    FROM all_currency_transactions AS act
    LEFT JOIN STV230530__STAGING.currencies as c ON act.currency_code=c.currency_code  and c.date_update::date=act.transaction_dt::date
    WHERE act.currency_code!=420 and c.currency_code_with=420),

    all_transactions AS(
    SELECT transaction_dt::date as date_update, operation_id, currency_from, amount, account_number_from FROM dollar_currency_transactions
    UNION ALL 
    SELECT transaction_dt::date as date_update, operation_id, currency_from, amount, account_number_from FROM non_dollar_currency_transactions)
    
    SELECT   
        date_update,
        currency_from, 
        sum(amount) as amount_total, 
        count(*) as cnt_transactions,  
        round(sum(amount)/count(distinct account_number_from),2) as avg_transactions_per_account, 
        count(distinct account_number_from) as cnt_accounts_make_transactions
    FROM all_transactions
    GROUP BY  date_update, currency_from) AS ttb
    
ON ttb.date_update=gm.date_update

WHEN MATCHED THEN UPDATE SET
                currency_from = ttb.currency_from,
                amount_total = ttb.amount_total, 
                cnt_transactions = ttb.cnt_transactions,  
                avg_transactions_per_account = ttb.avg_transactions_per_account,
                cnt_accounts_make_transactions = ttb.cnt_accounts_make_transactions

WHEN NOT MATCHED
    THEN INSERT (
                date_update,
                currency_from,
                amount_total, 
                cnt_transactions,  
                avg_transactions_per_account,
    cnt_accounts_make_transactions
        )
    VALUES (
                ttb.date_update,
                ttb.currency_from,
                ttb.amount_total, 
                ttb.cnt_transactions,  
                ttb.avg_transactions_per_account,
    ttb.cnt_accounts_make_transactions
        );
