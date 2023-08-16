--DROP TABLE IF EXISTS STV230530__STAGING.currencies CASCADE;
--DROP PROJECTION IF EXISTS STV230530__STAGING.currencies_proj1 CASCADE;
CREATE TABLE STV230530__STAGING.currencies
(
    date_update timestamp not null,
	currency_code int not null,
    currency_code_with int not null,
    
    currency_with_div numeric(3,2) not null,
    
    CONSTRAINT C_PRIMARY PRIMARY KEY (date_update, currency_code, currency_code_with) ENABLED 
)
	ORDER BY currency_code, currency_code_with
SEGMENTED BY HASH(currency_code, currency_code_with,date_update) ALL NODES
PARTITION BY date_update::DATE;
 


CREATE PROJECTION STV230530__STAGING.currencies_proj1 
(
 date_update,
 currency_code,
 currency_code_with,
 
 currency_with_div 
)
AS
 SELECT 
 	currencies.date_update,
 		currencies.currency_code,
        currencies.currency_code_with,
        
        currencies.currency_with_div 
 FROM STV230530__STAGING.currencies
 ORDER BY currencies.date_update 
SEGMENTED BY hash(currencies.date_update) ALL NODES KSAFE 1;


--SELECT MARK_DESIGN_KSAFE(1);

