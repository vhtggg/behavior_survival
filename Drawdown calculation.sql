create table td_study as 
with tmp as (
    select
        date,
        date - first_open_date_merge as age,
        acct_merge as acct,
        
        -- take cumulative min so as to remove increase principal
        min(principal) 
            over(partition by acct_merge order by date rows unbounded preceding) as princ
    from (
    
        -- join with rolldiff_acct_tmp to find original_open_date
        select
            a.*,
            (case when b.close_acct is null then a.acct_no else b.close_acct end) as acct_merge,
            (case when b.original_open_date is null then a.first_open_date else b.original_open_date end) as first_open_date_merge
        from 
            portfolio a
        left join 
            rolldiff_acct_tmp b
        on a.acct_no = b.open_acct
    )
    where 
        --segmentation
        currency  = 'VND' and
        term = '1' and
        act_type = 'TK CO KY HAN LAI CUOI KY' and
        
        --split date
        date >= '1-jan-2015' and
        date <= '31-dec-2018'
        
)
--
select age, sum(princ) as princ , sum(drawdown) as drawdown from (
    select 
        a.*, 
        --calculate drawdown
        lag(a.princ, 1) over (partition by acct order by age) - a.princ as drawdown 
    from (
        select age, acct, princ from tmp
            union all    
        --create 0 balance records of closed account
        select 
            age + 1 as age ,
            acct, 
            case when 1=1 then 0 end as princ
        from (
            
            --take last date of each account
            select
                tmp.*, row_number() over(partition by acct order by age desc) as rk
            from tmp
        ) where rk = 1 and date <> (select max(date) from tmp)  
    ) a
) group by age order by age;


create table td_study as 
with tmp as (
    select
        date,
        date - first_open_date_merge as age,
        acct_merge as acct,
        
        -- take cumulative min so as to remove increase principal
        min(principal) 
            over(partition by acct_merge order by date rows unbounded preceding) as princ
    from (
    
        -- join with rolldiff_acct_tmp to find original_open_date
        select
            a.*,
            (case when b.close_acct is null then a.acct_no else b.close_acct end) as acct_merge,
            (case when b.original_open_date is null then a.first_open_date else b.original_open_date end) as first_open_date_merge
        from 
            portfolio a
        left join 
            rolldiff_acct_tmp b
        on a.acct_no = b.open_acct
    )
    where
        --segmentation 
        currency  = 'VND' and
        term = '1' and
        act_type = 'TK CO KY HAN LAI CUOI KY' and
        --split date
        date >= '1-jan-2019' and
        date <= '31-dec-2019'
)
--
select age, sum(princ) as princ , sum(drawdown) as drawdown from (
    select 
        a.*, 
        --calculate drawdown
        lag(a.princ, 1) over (partition by acct order by age) - a.princ as drawdown 
    from (
        select age, acct, princ from tmp
            union all    
        --create 0 balance records of closed account
        select 
            age + 1 as age ,
            acct, 
            case when 1=1 then 0 end as princ
        from (
            
            --take last date of each account
            select
                tmp.*, row_number() over(partition by acct order by age desc) as rk
            from tmp
        ) where rk = 1 and date <> (select max(date) from tmp)  
    ) a
) group by age order by age;
