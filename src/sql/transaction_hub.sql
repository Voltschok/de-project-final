drop table if exists STV230530__DWH.h_transactions CASCADE;


create table STV230530__DWH.h_transactions
(
    hk_transaction_id bigint primary key,
    transaction_id      int,
    transaction_dt datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_transaction_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


drop table if exists STV230530__DWH.h_currencies CASCADE;

create table STV230530__DWH.h_currencies
(
    hk_currency_id bigint primary key,
    currency_code   int,
    date_update datetime,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_currency_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
 

drop table if exists STV230530__DWH.h_accounts CASCADE;

create table STV230530__DWH.h_accounts
(
    hk_account_id bigint primary key,
    account_number   int,
    load_dt datetime,
    load_src varchar(20)
)
order by load_dt
SEGMENTED BY hk_account_id all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

 
