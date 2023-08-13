create table STV230530__DWH.global_metrics
(
    date_update datetime,
    currency_from int not null,
    amount_total  numeric(14,3) not null,
    cnt_transactions int not null,
    avg_transactions_per_account numeric(14,3) not null,
	cnt_accounts_make_transactions int not null
);
