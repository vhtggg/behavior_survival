create table rolldiff as 
-- open_acct table = 1st day of each account
with open_acct as (
    select 
        client_no,
        currency,
        term,
        act_type, 
        open_date,
        count(distinct acct_no) as count_open,
        sum(principal_amt_base) as balance_open
    from (
        select * from (
            select 
                a.sym_run_date,
                a.acct_no,
                a.client_no,
                a.currency,
                a.term,
                a.act_type, 
                a.first_open_date as open_date,
                a.principal_amt_base,
                row_number() over(partition by a.acct_no, a.client_no order by a.sym_run_date) as rk
            from portfolio a
        ) where 
        rk = 1 and
        sym_run_date > (select min(sym_run_date) from portfolio)
    )group by 
        client_no,
        open_date,
        currency,
        term,
        act_type
), -- close_acct table = last day of each account
close_acct as (
    select 
        client_no,
        currency,
        term,
        act_type, 
        close_date,
        count(distinct acct_no) as count_close,
        sum(principal_amt_base) as balance_close
    from (
        select * from (
            select 
                a.sym_run_date,
                a.acct_no,
                a.client_no,
                a.currency,
                a.term,
                a.act_type, 
                a.sym_run_date as close_date,
                a.principal_amt_base,
                row_number() over(partition by a.acct_no, a.client_no order by a.sym_run_date desc) as rk
            from portfolio a
        ) where 
        rk = 1 and
        sym_run_date < (select max(sym_run_date) from portfolio)
    )group by 
        client_no,
        close_date,
        currency,
        term,
        act_type--, 
)
--final view = inner join open and close table
select 
    a.*,
    b.count_close,
    b.balance_close
from open_acct a
inner join
    close_acct b
on 
a.open_date = b.close_date + 1 and
a.client_no = b.client_no and
a.currency = b.currency and
a.term = b.term and
a.act_type = b.act_type
;



create table rolldiff_acct as
-- acct_max_age = max_age of each account w/out rolldiff
with acct_max_age as (
    select
        acct_no, max(sym_run_date - first_open_date) as max_age
    from portfolio
    group by acct_no
), 
-- open_rank = open table with rank balance on each roll date
open_rank as (
    select 
        a.*,
        row_number() 
            over(
                partition by 
                    currency, 
                    term, 
                    act_type, 
                    client_no, 
                    open_date 
                order by 
                    balance desc, max_age desc
            ) as rk
    from (
        select
            a.open_date,
            a.acct_no,
            b.client_no,
            b.currency,
            b.term,
            b.act_type,
            c.max_age,
            a.principal_amt_base as balance
        from (
        select * from (
            select 
                a.sym_run_date,
                a.acct_no,
                a.client_no,
                a.currency,
                a.term,
                a.act_type, 
                a.first_open_date as open_date,
                a.principal_amt_base,
                row_number() over(partition by a.acct_no, a.client_no order by a.sym_run_date) as rk
            from portfolio a
        ) where 
        rk = 1 and
        sym_run_date > (select min(sym_run_date) from portfolio)
        ) a inner join (
            rolldiff
        ) b 
        on 
        a.client_no = b.client_no and
        a.currency = b.currency and
        a.term = b.term and
        a.act_type = b.act_type and
        a.open_date = b.open_date
        left join (
            acct_max_age
        ) c
        on 
        a.acct_no = c.acct_no
    )a
), 
-- close_rank = close table with rank balance on each roll date
close_rank as (
    select 
        a.*,
        row_number() 
            over(
                partition by 
                    currency, 
                    term, 
                    act_type, 
                    client_no, 
                    close_date 
                order by 
                    balance desc, max_age desc
            ) as rk
    from (
        select
            a.close_date,
            a.acct_no,
            b.client_no,
            b.currency,
            b.term,
            b.act_type,
            a.principal_amt_base as balance,
            c.max_age,
            a.original_open_date
        from (
        select * from (
            select 
                a.sym_run_date,
                a.acct_no,
                a.client_no,
                a.currency,
                a.term,
                a.act_type, 
                a.sym_run_date as close_date,
                a.first_open_date as original_open_date,
                a.principal_amt_base,
                row_number() over(partition by a.acct_no, a.client_no order by a.sym_run_date desc) as rk
            from portfolio a
        ) where 
        rk = 1 and
        sym_run_date < (select max(sym_run_date) from portfolio)
        ) a inner join (
            rolldiff
        ) b 
        on 
        a.client_no = b.client_no and
        a.currency = b.currency and
        a.term = b.term and
        a.act_type = b.act_type and
        a.close_date = b.open_date - 1
        left join (
            acct_max_age
        ) c
        on 
        a.acct_no = c.acct_no
    )a

) 

-- final view = open_rank inner join close_rank 
select 
    a.client_no,
    a.currency, 
    a.term,
    a.act_type,
    a.open_date,
    b.close_date,
    b.acct_no as close_acct,
    a.acct_no as open_acct,
    b.original_open_date,
    a.rk as open_rank,
    b.rk as close_rank,
    a.balance as open_balance,
    b.balance as close_balance,
    a.max_age as open_max_age,
    b.max_age as close_max_age
    
from 
    open_rank a
inner join
    close_rank b
on
    a.client_no = b.client_no and
    a.currency = b.currency and
    a.term = b.term and
    a.act_type = b.act_type and
    a.open_date = b.close_date + 1 and
    a.rk = b.rk
;






-- multi roll account
--create rolldiff_acct_tmp as rolldiff_acct + first open date of each close acct
create table rolldiff_acct_tmp as select close_acct, open_acct, original_open_date from rolldiff_acct;

-- loop join rolldiff_acct_tmp with itself until no case left (no same account in close_acct and open_acct column)
declare 
    n number; 
begin 
select 
    z into n 
from (
    select count(*) z from (
        select close_acct from rolldiff_acct_tmp 
        intersect 
        select open_acct from rolldiff_acct_tmp
    )
);

while n > 0 
loop
    execute immediate  
    'create table tmp as (
    
    select 
        case when b.close_acct is null then a.close_acct else b.close_acct end as close_acct,
        a.open_acct,
        case when b.original_open_date is null then a.original_open_date else b.original_open_date end as original_open_date
    from 
        rolldiff_acct_tmp a 
    left join
        rolldiff_acct_tmp b
    on a.close_acct = b.open_acct
    
    )';
    
    execute immediate 'drop table rolldiff_acct_tmp';
    
    execute immediate 'create table rolldiff_acct_tmp as select * from tmp';
    
    execute immediate 'drop table tmp';
    
    select z into n from (
        select count(*) z from (
            select close_acct from rolldiff_acct_tmp 
            intersect 
            select open_acct from rolldiff_acct_tmp
        )
    );


end loop;
end; 



















