-- Using the tables

-- what was the largest daily portfolio move on a percentage basis?  
select p.CloseDate
	 , p.pl / n_prev.nav as daily_return
	from pl p
		inner join nav n_prev on n_prev.closedate = date(p.closedate, '-1 day')
order by abs(daily_return) desc
limit 1
		

-- what were the ticker PL contributions for the time period?

select p.Ticker 	
	 , sum((cp.closeprice - cp_prev.closeprice) * p.quantity) as ticker_pl
from positions p
	inner join calendar c on c.closedate = p.CloseDate 
	inner join closeprices cp_prev on cp_prev.closedate = c.previous_business_date and p.ticker = cp_prev.ticker
	inner join closeprices cp on cp.closedate = c.closedate and cp.ticker = p.ticker 
group by p.ticker
order by ticker_pl desc

-- use the calendar table to calcuate the daily percentage changes for each ticker
SELECT cp.CloseDate,
       cp.Ticker,
       (cp.ClosePrice - cp_prev.ClosePrice) * 1.0 / cp_prev.ClosePrice AS Daily_Return
FROM Calendar c
    INNER JOIN ClosePrices cp
        ON cp.CloseDate = c.CloseDate
    INNER JOIN ClosePrices cp_prev
        ON cp_prev.Ticker = cp.Ticker
       AND cp_prev.CloseDate = c.Previous_Business_Date


-- create a Z score for each tickers move, use a 90 day look back.  What was the largest move on an absolute basis?
with daily_ret as (
    select
        cp.closedate,
        cp.ticker,
        (cp.closeprice - cp_prev.closeprice) * 1.0 / cp_prev.closeprice as daily_return
    from calendar c
    join closeprices cp
        on cp.closedate = c.closedate
    join closeprices cp_prev
        on cp_prev.ticker = cp.ticker
       and cp_prev.closedate = c.previous_business_date
),
z_with_window as (
    select
        dr.closedate,
        dr.ticker,
        dr.daily_return,
        count(*) over (
            partition by dr.ticker
            order by dr.closedate
            rows between 89 preceding and current row
        ) as cnt_90,
        avg(dr.daily_return) over (
            partition by dr.ticker
            order by dr.closedate
            rows between 89 preceding and current row
        ) as mean_90,
        avg(dr.daily_return * dr.daily_return) over (
            partition by dr.ticker
            order by dr.closedate
            rows between 89 preceding and current row
        ) as mean_sq_90
    from daily_ret dr
),
z_final as (
    select
        closedate,
        ticker,
        daily_return,
        (daily_return - mean_90) /
        nullif(
            sqrt(mean_sq_90 - mean_90 * mean_90),
            0
        ) as z_score
    from z_with_window
    where cnt_90 >= 90
)
select
    closedate,
    ticker,
    daily_return,
    z_score
from z_final
order by abs(z_score) desc
limit 1;



-- use a 30 standard deviation to measure risk, what date and ticker was the most risk allocated to a position?
with daily_ret as (
    select
        cp.closedate,
        cp.ticker,
        (cp.closeprice - cp_prev.closeprice) * 1.0 / cp_prev.closeprice as daily_return
    from calendar c
    join closeprices cp
        on cp.closedate = c.closedate
    join closeprices cp_prev
        on cp_prev.ticker = cp.ticker
       and cp_prev.closedate = c.previous_business_date
),
vol_with_window as (
    select
        dr.closedate,
        dr.ticker,
        dr.daily_return,
        count(*) over (
            partition by dr.ticker
            order by dr.closedate
            rows between 29 preceding and current row
        ) as cnt_30,
        avg(dr.daily_return) over (
            partition by dr.ticker
            order by dr.closedate
            rows between 29 preceding and current row
        ) as mean_30,
        avg(dr.daily_return * dr.daily_return) over (
            partition by dr.ticker
            order by dr.closedate
            rows between 29 preceding and current row
        ) as mean_sq_30
    from daily_ret dr
),
vol_30 as (
    select
        closedate,
        ticker,
        sqrt(mean_sq_30 - mean_30 * mean_30) as std_30
    from vol_with_window
    where cnt_30 >= 30
),
risk as (
    select
        p.closedate,
        p.ticker,
        p.quantity,
        cp.closeprice,
        v.std_30,
        abs(p.quantity * cp.closeprice * v.std_30) as risk_amount
    from positions p
    join closeprices cp
        on cp.closedate = p.closedate
       and cp.ticker   = p.ticker
    join vol_30 v
        on v.closedate = p.closedate
       and v.ticker    = p.ticker
)
select
    closedate,
    ticker,
    quantity,
    closeprice,
    std_30,
    risk_amount
from risk
order by risk_amount desc
limit 1;



-- calculate the portfolio net and gross (abs) notional exposure for each day, what is the average net/gross exposure?  Which date did the portfolio have the largest short exposure?
with daily_exposure as (
    select
        p.closedate,
        sum(p.quantity * cp.closeprice) as net_notional,
        sum(abs(p.quantity * cp.closeprice)) as gross_notional,
        sum(
            case
                when p.quantity < 0 then abs(p.quantity * cp.closeprice)
                else 0
            end
        ) as short_notional
    from positions p
    join closeprices cp
        on cp.closedate = p.closedate
       and cp.ticker   = p.ticker
    group by p.closedate
)

-- 1a) net and gross notional per day
select *
from daily_exposure
order by closedate;

-- 1b) average net / gross exposure
select
    avg(net_notional)   as avg_net_notional,
    avg(gross_notional) as avg_gross_notional
from daily_exposure;

-- 1c) date with largest short exposure
select
    closedate,
    short_notional
from daily_exposure
order by short_notional desc
limit 1;



-- what are the monthly returns for the portfolio?
with nav_by_month as (
    select
        strftime('%Y-%m', closedate) as ym,
        min(closedate) as month_start_date,
        max(closedate) as month_end_date
    from nav
    group by strftime('%Y-%m', closedate)
),
nav_ends as (
    select
        m.ym,
        ns.nav as nav_start,
        ne.nav as nav_end
    from nav_by_month m
    join nav ns
        on ns.closedate = m.month_start_date
    join nav ne
        on ne.closedate = m.month_end_date
)
select
    ym,
    (nav_end * 1.0 / nav_start - 1.0) as monthly_return
from nav_ends
order by ym;


-- calculate the rolling 30 and 90 day portfolio returns
select
    closedate,
    nav,
    (nav * 1.0 / lag(nav, 29) over (order by closedate) - 1.0) as ret_30d,
    (nav * 1.0 / lag(nav, 89) over (order by closedate) - 1.0) as ret_90d
from nav
order by closedate;



-- what is the worst drawdown for the portfolio?
with nav_peak as (
    select
        closedate,
        nav,
        max(nav) over (
            order by closedate
            rows between unbounded preceding and current row
        ) as peak_nav
    from nav
),
dd as (
    select
        closedate,
        nav,
        peak_nav,
        nav * 1.0 / peak_nav - 1.0 as drawdown
    from nav_peak
)
select
    closedate,
    nav,
    peak_nav,
    drawdown
from dd
order by drawdown
limit 1;



-- what are the best and worst trades as of the last day we have pricing data?
with last_price_date as (
    select max(closedate) as last_date
    from closeprices
),
trade_pnl as (
    select
        t.orderid,
        t.tradedate,
        t.ticker,
        t.shares,
        t.action,
        t.tradeprice,
        cp.last_closeprice,
        (cp.last_closeprice - t.tradeprice) *
        case
            when lower(t.action) = 'sell' then -t.shares
            else t.shares
        end as trade_pnl
    from trades t
    join (
        select
            lp.last_date,
            cp_inner.ticker,
            cp_inner.closeprice as last_closeprice
        from last_price_date lp
        join closeprices cp_inner
            on cp_inner.closedate = lp.last_date
    ) cp
        on cp.ticker = t.ticker
)
-- best trade (highest pnl) and worst trade (lowest pnl)
select *
from trade_pnl
order by trade_pnl desc
limit 1;

select *
from trade_pnl
order by trade_pnl asc
limit 1;






