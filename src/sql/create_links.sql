drop table if exists STV230530__DWH.l_transaction_account CASCADE;

create table STV230530__DWH.l_transaction_account (
hk_l_transaction_account bigint primary key,
hk_transaction_id bigint not null CONSTRAINT fk_l_transaction_account_transaction  REFERENCES STV230530__DWH.h_transactions (hk_transaction_id),
hk_account_id bigint not null CONSTRAINT fk_l_transaction_account_account   REFERENCES STV230530__DWH.h_accounts (hk_account_id),
load_dt datetime,
load_src varchar(20)
)

order by load_dt
SEGMENTED BY hk_transaction_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists STV230530__DWH.l_transaction_currency CASCADE;

create table STV230530__DWH.l_transaction_currency (
hk_l_transaction_currency bigint primary key,
hk_transaction_id bigint not null CONSTRAINT fk_l_transaction_currency_transaction  REFERENCES STV230530__DWH.h_transactions (hk_transaction_id),
hk_currency_id bigint not null CONSTRAINT fk_l_transaction_currency_currency   REFERENCES STV230530__DWH.h_currencies (hk_currency_id),
load_dt datetime,
load_src varchar(20)
)

order by load_dt
SEGMENTED BY hk_transaction_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
