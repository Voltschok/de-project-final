-- STV230530.members definition
--DROP TABLE STV230530__STAGING.transactions;
--DROP PROJECTION STV230530__STAGING.transactions_proj1;
CREATE TABLE STV230530__STAGING.transactions
(
    operation_id uuid NOT NULL,
 
    account_number_from int not null,
    account_number_to int not null,
    currency_code int not null,
    country varchar(200) not null,
    status varchar (50) not null,
    transaction_type varchar (50) not null,
    amount int not null, 
    transaction_dt timestamp, 
    CONSTRAINT C_PRIMARY PRIMARY KEY (operation_id)  
);


CREATE PROJECTION STV230530__STAGING.transactions_proj1 /*+createtype(L)*/ 
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
 ORDER BY transactions.operation_id, transactions.transaction_dt
SEGMENTED BY hash(transactions.operation_id, transactions.transaction_dt) ALL NODES KSAFE 1;


--SELECT MARK_DESIGN_KSAFE(1);
