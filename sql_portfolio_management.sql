--Portfolio Management

--* Creating the important tables


--select cp.CloseDate 
--, cp.Ticker 
--, cp.ClosePrice 
--, cp.volume

select *
from closeprices cp 
		inner join Calendar c  on c.closedate = cp.closedate
		inner join closeprices cp_prev on cp_prev.closedate = c.previous_business_date
									and cp.ticker = cp_prev.ticker
where cp.ticker = 'AAPL' and cp.closedate = '2023-12-14'

	
-------------------------------------------------------------------------------------------------------------------
--avg price calculation

select c.* 
	, cp.ClosePrice
	, t.*
from Calendar c 
	inner join ClosePrices cp on cp.CloseDate = c.CloseDate 
	left join Trades t on t.TradeDate = c.CloseDate
						and t.Ticker = cp.Ticker 
	

select cp.CloseDate 
	, cp.ClosePrice 
	, cp.ticker
	, cp_prev.ClosePrice as Prev_Close
from ClosePrices cp 
	inner join Calendar c on c.CloseDate = cp.CloseDate 
	inner join ClosePrices cp_prev on cp_prev.CloseDate = c.Previous_Business_Date 
									and cp_prev.Ticker = cp.Ticker 
order by c.CloseDate  asc


--how many trades and shares are traded each date
select c.CloseDate
	, COUNT(t.OrderID) as 'No_Trades'
	, SUM(IFNULL(t.Shares, 0)) as 'No_Shares'
from Calendar c 
	left join Trades t on t.TradeDate = c.CloseDate 
group by c.CloseDate



select strftime('%m', c.CloseDate) as 'Month'
	, strftime('%Y', c.CloseDate) as 'Year'
	, MIN(strftime('%d', c.CloseDate)) as 'Month_Start_Day'
	, MAX(strftime('%d', c.CloseDate)) as 'Month_End_Day'
from Calendar c 
group by strftime('%m', c.CloseDate), strftime('%Y', c.CloseDate)




-- calculate avg price change for each stock each month 
select cp.Ticker 
	, strftime('%m', cp.CloseDate) as 'Month'
	, AVG(cp.ClosePrice / cp_prev.ClosePrice - 1) as 'AVG_percent_change'
from Calendar c 
	inner join ClosePrices cp on cp.CloseDate = c.CloseDate 
	inner join ClosePrices cp_prev on cp_prev.CloseDate = c.Previous_Business_Date 
										and cp_prev.Ticker = cp.Ticker 
group by cp.Ticker, strftime('%m', cp.CloseDate)
order by AVG(cp.ClosePrice / cp_prev.ClosePrice - 1) desc

------------------------------------------------------------------------------------------------
--Table Creation



--Positions_Starting_Query
-- need to use CASE WHEN to assign trade direction
-- SUM() these trades grouping by the Ticker and CloseDate (possible to have multiple trades for same ticker on same date)
-- 1) See all trades up to a given CloseDate
select c.CloseDate,
       t.*
from Calendar c
    inner join Trades t
        on t.TradeDate <= c.CloseDate
where c.CloseDate = '2023-01-28';


-- 2) Net quantity per ticker per date (buy = +, sell = -), tracks automatically through the dates, cumulative total number of shares
select c.CloseDate,
       t.Ticker,
       t.tradedate,
       SUM(
           CASE
               when t."Action" = 'Sell' then -t.Shares
               else t.Shares
           END
       ) as "Quantity"
from Calendar c
    inner join Trades t
        on t.TradeDate <= c.CloseDate
group by c.CloseDate, t.Ticker
order by c.CloseDate, t.Ticker;





--Position table, tracked daily, total holding shares
insert into positions (closedate, ticker, quantity)

select c.closedate
	, t.ticker
	,sum(case
		when t.'Action' = 'Sell' then -t.shares
		else t.shares
	end) as "quantity"
from calendar c
	inner join trades t on t.tradedate <= c.CloseDate 
group by c.closedate, t.ticker




--Position (Close Price Diff) PnL
select 
    p.*,
    cp_prev.ClosePrice as Previous_Day_ClosePrice,
    cp.ClosePrice as Todays_ClosePrice,
    sum( (cp.ClosePrice - cp_prev.ClosePrice) * p.Quantity ) as Position_PnL
from Positions p 
    inner join Calendar c 
        on c.CloseDate = p.CloseDate 
    inner join ClosePrices cp_prev 
        on cp_prev.CloseDate = c.Previous_Business_Date 
       and p.Ticker = cp_prev.Ticker 
    inner join ClosePrices cp 
        on cp.CloseDate = c.CloseDate 
       and cp.Ticker = cp_prev.Ticker
-- need to add current close prices and calculate position PL using the ClosePrices table
group by c.CloseDate,cp.ticker;




-- need to calculate the diff between the ClosePrice and the TradePrice 
-- multiply this diff by the Quantity_Traded
-- SUM() all of the PL and group by CloseDate


-- Position Trade Holding (Future close - Trade Price) PnL
select t.TradeDate 
	, t.ticker
	, sum(
	(cp.closeprice - t.tradeprice) * case 
											when t.action = 'Sell' then -t.shares
											else t.shares
										end 
										) as Trading_PnL
from Trades t 
	inner join ClosePrices cp on cp.CloseDate = t.TradeDate and cp.Ticker = t.Ticker 
group by t.tradedate, t.ticker
---------------------------------------------------------------------------------------------




--PL Table
	
insert into PL (closedate, PL)

with position_pl as (
	select p.CloseDate 
		, SUM((cp.ClosePrice - cp_prev.ClosePrice) * p.Quantity) as Position_PL
	from Positions p 
		inner join Calendar c on c.CloseDate = p.CloseDate 
		inner join ClosePrices cp_prev on cp_prev.CloseDate = c.Previous_Business_Date and p.Ticker = cp_prev.Ticker 
		inner join ClosePrices cp on cp.CloseDate = p.CloseDate  and cp.Ticker = p.Ticker 
	group by p.CloseDate 
	),
	
trade_pl as (

	
	select t.TradeDate 
		, SUM( (cp.ClosePrice - t.TradePrice)
		* case
			when t.Action = 'Sell' then -t.Shares
			else t.Shares
		end) as Trade_PL
	from Trades t 
		inner join ClosePrices cp on cp.CloseDate = t.TradeDate and cp.Ticker = t.Ticker 
	group by t.TradeDate 
	)
	
	
select c.CloseDate 
	, ifnull( p.position_PL,0) + ifnull(t. Trade_PL, 0) as'PL'
from Calendar c 
	left join position_pl p on p.CloseDate = c.CloseDate 
	left join trade_pl t on t.TradeDate = c.CloseDate 




-- NAV Table
insert into nav (closedate, nav)
select pl1.CloseDate
	, SUM(pl2.PL) + 100000 AS NAV --100k as starting NAV
from pl pl1
	left join pl pl2 on pl1.CloseDate >=pl2.CloseDate

group by pl1.CloseDate
order by pl1.CloseDate	


--select cp.CloseDate 
--, cp.Ticker 
--, cp.ClosePrice 
--, cp.volume

select *
from closeprices cp 
		inner join Calendar c  on c.closedate = cp.closedate
		inner join closeprices cp_prev on cp_prev.closedate = c.previous_business_date
									and cp.ticker = cp_prev.ticker
where cp.ticker = 'AAPL' and cp.closedate = '2023-12-14'



--------------------------------------------------------------------------


select c.* 
	, cp.ClosePrice
	, t.*
from Calendar c 
	inner join ClosePrices cp on cp.CloseDate = c.CloseDate 
	left join Trades t on t.TradeDate = c.CloseDate
						and t.Ticker = cp.Ticker 
	

select cp.CloseDate 
	, cp.ClosePrice 
	, cp_prev.ClosePrice 
from ClosePrices cp 
	inner join Calendar c on c.CloseDate = cp.CloseDate 
	inner join ClosePrices cp_prev on cp_prev.CloseDate = c.Previous_Business_Date 
									and cp_prev.Ticker = cp.Ticker 
order by c.CloseDate  asc


select c.CloseDate
	, COUNT(t.OrderID) as 'No_Trades'
	, SUM(IFNULL(t.Shares, 0)) as 'No_Shares'
from Calendar c 
	left join Trades t on t.TradeDate = c.CloseDate 
group by c.CloseDate



select strftime('%m', c.CloseDate) as 'Month'
	, strftime('%Y', c.CloseDate) as 'Year'
	, MIN(strftime('%d', c.CloseDate)) as 'Month_Start_Day'
	, MAX(strftime('%d', c.CloseDate)) as 'Month_End_Day'
from Calendar c 
group by strftime('%m', c.CloseDate), strftime('%Y', c.CloseDate)





select cp.Ticker 
	, strftime('%m', cp.CloseDate) as 'Month'
	, AVG(cp.ClosePrice / cp_prev.ClosePrice - 1) as 'AVG_percent_change'
from Calendar c 
	inner join ClosePrices cp on cp.CloseDate = c.CloseDate 
	inner join ClosePrices cp_prev on cp_prev.CloseDate = c.Previous_Business_Date 
										and cp_prev.Ticker = cp.Ticker 
group by cp.Ticker, strftime('%m', cp.CloseDate)
order by AVG(cp.ClosePrice / cp_prev.ClosePrice - 1) desc





-- Using the tables we created in class:

-- what was the largest daily portfolio move on a percentage basis?  

-- what were the ticker PL contributions for the time period?

-- use the calendar table to calcuate the daily percentage changes for each ticker

-- create a Z score for each tickers move, use a 90 day look back.  What was the largest move on an absolute basis?

-- use a 30 standard deviation to measure risk, what date and ticker was the most risk allocated to a position?

-- calculate the portfolio net and gross (abs) notional exposure for each day, what is the average net/gross exposure?  Which date did the portfolio have the largest short exposure?

-- what are the monthly returns for the portfolio?

-- calculate the rolling 30 and 90 day portfolio returns

-- what is the worst drawdown for the portfolio?

-- what are the best and worst trades as of the last day we have pricing data?


------------------------------------------------------------------------------------------

--Positions_Starting_Query
-- need to use CASE WHEN to assign trade direction
-- SUM() these trades grouping by the Ticker and CloseDate (possible to have multiple trades for same ticker on same date)
-- 1) See all trades up to a given CloseDate
select c.CloseDate,
       t.*
from Calendar c
    inner join Trades t
        on t.TradeDate <= c.CloseDate
where c.CloseDate = '2023-01-28';


-- 2) Net quantity per ticker per date (buy = +, sell = -)
select c.CloseDate,
       t.Ticker,
       SUM(
           CASE
               when t."Action" = 'Sell' then -t.Shares
               else t.Shares
           END
       ) as "Quantity"
from Calendar c
    inner join Trades t
        on t.TradeDate <= c.CloseDate
group by c.CloseDate, t.Ticker
order by c.CloseDate, t.Ticker;





--Position table
insert into positions (closedate, ticker, quantity)

select c.closedate
	, t.ticker
	,sum(case
		when t.'Action' = 'Sell' then -t.shares
		else t.shares
	end) as "quantity"
from calendar c
	inner join trades t on t.tradedate <= c.CloseDate 
group by c.closedate, t.ticker


--PnL
select 
    p.*,
    cp_prev.ClosePrice as Previous_Day_ClosePrice,
    cp.ClosePrice as Todays_ClosePrice,
    sum( (cp.ClosePrice - cp_prev.ClosePrice) * p.Quantity ) as Position_PnL
from Positions p 
    inner join Calendar c 
        on c.CloseDate = p.CloseDate 
    inner join ClosePrices cp_prev 
        on cp_prev.CloseDate = c.Previous_Business_Date 
       and p.Ticker = cp_prev.Ticker 
    inner join ClosePrices cp 
        on cp.CloseDate = c.CloseDate 
       and cp.Ticker = cp_prev.Ticker
-- need to add current close prices and calculate position PL using the ClosePrices table
group by c.CloseDate;




-- need to calculate the diff between the ClosePrice and the TradePrice 
-- multiply this diff by the Quantity_Traded
-- SUM() all of the PL and group by CloseDate

select t.TradeDate 
	, sum(
	(cp.closeprice - t.tradeprice) * case 
											when t.action = 'Sell' then -t.shares
											else t.shares
										end 
										) as Trading_PnL
from Trades t 
	inner join ClosePrices cp on cp.CloseDate = t.TradeDate and cp.Ticker = t.Ticker 
group by t.tradedate
---------------------------------------------------------------------------------------------

--PL Table
	
insert into PL (closedate, PL)

with position_pl as (
	select p.CloseDate 
		, SUM((cp.ClosePrice - cp_prev.ClosePrice) * p.Quantity) as Position_PL
	from Positions p 
		inner join Calendar c on c.CloseDate = p.CloseDate 
		inner join ClosePrices cp_prev on cp_prev.CloseDate = c.Previous_Business_Date and p.Ticker = cp_prev.Ticker 
		inner join ClosePrices cp on cp.CloseDate = p.CloseDate  and cp.Ticker = p.Ticker 
	group by p.CloseDate 
	),
	
trade_pl as (

	
	select t.TradeDate 
		, SUM( (cp.ClosePrice - t.TradePrice)
		* case
			when t.Action = 'Sell' then -t.Shares
			else t.Shares
		end) as Trade_PL
	from Trades t 
		inner join ClosePrices cp on cp.CloseDate = t.TradeDate and cp.Ticker = t.Ticker 
	group by t.TradeDate 
	)
	
	

select c.CloseDate 
	, ifnull( p.position_PL,0) + ifnull(t. Trade_PL, 0) as'PL'
from Calendar c 
	left join position_pl p on p.CloseDate = c.CloseDate 
	left join trade_pl t on t.TradeDate = c.CloseDate 



-- NAV Table
insert into nav (closedate, nav)
select pl1.CloseDate
	, SUM(pl2.PL) + 100000 AS NAV --100k as starting NAV
from pl pl1
	left join pl pl2 on pl1.CloseDate >=pl2.CloseDate

group by pl1.CloseDate
order by pl1.CloseDate	
