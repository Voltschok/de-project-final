drop table if exists STV230530__DWH.s_currency;

create table STV230530__DWH.s_currency_exchange_rate
(
hk_currency_id bigint not null CONSTRAINT fk_s_currency_h_currencies REFERENCES STV230530__DWH.h_currencies (hk_currency_id),
currency_code_with int not null,
date_update datetime not null,
currency_with_div numeric(3,2),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_currency_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists STV230530__DWH.s_transactions_country;

create table STV230530__DWH.s_transactions_country
(
hk_transaction_id bigint not null CONSTRAINT fk_s_transactions_country_h_transactions REFERENCES STV230530__DWH.h_transactions (hk_transaction_id),
country varchar(50),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_transaction_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists STV230530__DWH.s_transactions_status;

create table STV230530__DWH.s_transactions_status
(
hk_transaction_id bigint not null CONSTRAINT fk_s_transactions_status_h_transactions REFERENCES STV230530__DWH.h_transactions (hk_transaction_id),
status varchar(50),
transaction_dt datetime,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_transaction_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

drop table if exists STV230530__DWH.s_transactions_amount;

create table STV230530__DWH.s_transactions_amount
(
hk_transaction_id bigint not null CONSTRAINT fk_s_transactions_amount_h_transactions REFERENCES STV230530__DWH.h_transactions (hk_transaction_id),
amount numeric(15,2),
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_transaction_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


drop table if exists STV230530__DWH.s_transactions_type;

create table STV230530__DWH.s_transactions_type
(
hk_transaction_id bigint not null CONSTRAINT fk_s_transactions_type_h_transactions REFERENCES STV230530__DWH.h_transactions (hk_transaction_id),
transaction_type varchar(50) not null ,
load_dt datetime,
load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_transaction_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

