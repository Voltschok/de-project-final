-- STV230530.members definition
DROP TABLE STV230530__STAGING.currencies;
DROP PROJECTION STV230530__STAGING.currencies_proj1;
CREATE TABLE STV230530__STAGING.currencies
(
  
	currency_code int not null,
    currency_code_with int not null,
    date_update timestamp not null,
    currency_with_div numeric(3,2) not null,
    
    CONSTRAINT C_PRIMARY PRIMARY KEY (date_update, currency_code, currency_code_with) DISABLED
);


CREATE PROJECTION STV230530__STAGING.currencies_proj1 /*+createtype(L)*/ 
(
 
 currency_code,
 currency_code_with,
 date_update,
 currency_with_div 
)
AS
 SELECT 
 		currencies.currency_code,
        currencies.currency_code_with,
        currencies.date_update,
        currencies.currency_with_div 
 FROM STV230530__STAGING.currencies
 ORDER BY currencies.date_update 
SEGMENTED BY hash(currencies.date_update) ALL NODES KSAFE 1;


--SELECT MARK_DESIGN_KSAFE(1);
