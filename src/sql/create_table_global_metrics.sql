-- DROP TABLE IF EXISTS STV230530__DWH.global_metrics CASCADE;
CREATE TABLE STV230530__DWH.global_metrics
(
    date_update date,
    currency_from int not null,
    amount_total  numeric(16,2) not null,
    cnt_transactions int not null,
    avg_transactions_per_account numeric(16,2) not null,
	cnt_accounts_make_transactions int not null
);
