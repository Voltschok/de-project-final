--DROP TABLE IF EXISTS STV230530__STAGING.transactions CASCADE;
--DROP PROJECTION IF EXISTS STV230530__STAGING.transactions_proj1 CASCADE;
CREATE TABLE STV230530__STAGING.transactions
(
    operation_id uuid NOT NULL,
 
    account_number_from int not null,
    account_number_to int not null,
    currency_code int not null,
    country varchar(2000) not null,
    status varchar (500) not null,
    transaction_type varchar (500) not null,
    amount int not null, 
    transaction_dt timestamp, 
    CONSTRAINT C_PRIMARY PRIMARY KEY (operation_id, status, transaction_dt)  DISABLED
) 
ORDER BY operation_id
SEGMENTED BY HASH(operation_id,transaction_dt) ALL NODES
PARTITION BY transaction_dt::DATE;
 
    
CREATE PROJECTION STV230530__STAGING.transactions_proj1 
(
 operation_id,
 account_number_from,
 account_number_to,
 currency_code,
 country,
 status,
 transaction_type,
 amount,
 transaction_dt 
)
AS
 SELECT transactions.operation_id,
 		transactions.account_number_from,
        transactions.account_number_to,
        transactions.currency_code,
        transactions.country,
        transactions.status,
        transactions.transaction_type,
        transactions.amount,
        transactions.transaction_dt
 FROM STV230530__STAGING.transactions
 ORDER BY transactions.transaction_dt
SEGMENTED BY hash( transactions.transaction_dt) ALL NODES KSAFE 1;


--SELECT MARK_DESIGN_KSAFE(1);
