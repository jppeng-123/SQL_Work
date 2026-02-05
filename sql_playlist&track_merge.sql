-- playlist&track_merge project

-- on playlist 8, which artist as the most tracks?
select a2.Name, a.title 
	, count(pt.trackid) "number_songs"
from playlisttrack pt
	inner join track t on t.trackid = pt.trackid
	inner join album a on a.albumid = t.albumid
	inner join artist a2 on a2.Artistid = a.ArtistId 
where pt.playlistid = 8
group by a2.name, a.title
order by count(pt.TrackId ) desc



with p_list as (
select a2.Name, a.title 
	, count(pt.trackid) "number_songs"
from playlisttrack pt
	inner join track t on t.trackid = pt.trackid
	inner join album a on a.albumid = t.albumid
	inner join artist a2 on a2.Artistid = a.ArtistId 
where pt.playlistid = 8
group by a2.name, a.title

union all

select a2.Name, a.title 
	, count(pt.trackid) "number_songs"
from playlisttrack pt
	inner join track t on t.trackid = pt.trackid
	inner join album a on a.albumid = t.albumid
	inner join artist a2 on a2.Artistid = a.ArtistId 
where pt.playlistid = 9
group by a2.name, a.title
)
select * from p_list




--Display all the track names from the tracks table where the genre name contains rap and the track composer is not null
select *
from track t
inner join genre g on g.genreid = t.genreid and g.name like '%rap%'



select *
from track t
	inner join genre g on g.genreid = t.GenreId and g.name like '%rap%'
where t.composer is not null 


--Playlistid 1, what is the cost of each of the 'Iron Maiden', 'Pearl Jam', and 'Foo Fighters' Albums
select a2.name, a.title, sum(t.unitprice)
from playlisttrack pt
	inner join track t on t.trackid = pt.trackid
	inner join album a on a.albumid = t.albumid
	inner join artist a2 on a2.Artistid = a.ArtistId and a2.name in ('Iron Maiden', 'Pearl Jam', 'Foo Fighters')
where pt.playlistid=1  --or include and a2.name in ('Iron Maiden', 'Pearl Jam', 'Foo Fighters') right here 
group by a2.name, a.title
order by sum(t.unitprice) desc


-- which artists has self titled albums?
select *
from album a
	inner join artist a2 on a2.artistid = a.ArtistId 
where a.title = a2.name 









---->> Basic SQL operations

-- Demonstrate how to select * from the Track table
select *
from track

-- Demonstrate how to order by the Track Name column
select t.name
from track t
order by t.name desc

-- Demonstrate how to filter the data by using each: where, like, in, <>, between, and columns equal to each other
select *
from track t
where t.unitprice > 0.01                          -- WHERE
  and t.genreid in (1,2,3,4,5)                        -- IN
  and t.name like '%a%'                          -- LIKE
  and t.milliseconds between 100000 and 500000    -- BETWEEN
  and t.albumid <> 4                              -- <>
  and t.trackid = t.mediaTypeId                   -- column = column
;



-- Demonstrate how to join the Track table on the Album and Genre table
select *
from track t
	inner join album al on al.albumid = t.albumid
	inner join genre ge on ge.genreid = t.genreid

-- Demonstrate how to display the Track Name, Album, and everything from the Genre table (*)
select 
    t.Name as Track_Name,
    al.Title as Album_Name,
    ge.Name as Genre
from track t
	inner join album al on al.albumid = t.albumid
	inner join genre ge on ge.genreid = t.genreid
	
-- Demonstrate how to use each: sum, max, min, avg, count -> group by a Genre to aggregate the information by 
select  al.Title as Album_Name,
    	ge.Name as Genre,
    	sum(t.bytes),
    	min(t.bytes),
    	max(t.bytes),
    	avg(t.bytes),
    	count(t.bytes)
from track t
	inner join album al on al.albumid = t.albumid
	inner join genre ge on ge.genreid = t.genreid
group by album_name, genre
	

---->> External Data 

-- Create new tables and insert the data from the course repo.  If you have already created these drop and recreate the tables
---- MA590_Fall_2025/python/dow_historical_data.csv
---- MA590_Fall_2025/python/dow_members.csv

drop table if exists dow_historical_data;
drop table if exists dow_members


-- Left join the data table on itself and calculate the moving 30 day average.  HINT: you may need to use date(dhd.Date, '-30 days')
-- How can we exclude average calculations when there are less than 18 observations in the look back?

select dhd.date
	,dhd."close"
	,dhd.ticker
	,avg(dhd_prev."Close")

from dow_historical_data dhd
	left join dow_historical_data dhd_prev on dhd.ticker = dhd_prev.ticker
											and dhd_prev.date >= date(dhd.date, '-30 days')
											and dhd_prev.date <= dhd.date
											
group by dhd.date, dhd."Close", dhd.ticker
having count(dhd_prev.close) >= 18



---- Business case example: "Should we align support reps to region to improve efficiency?"


-- display the distinct SupportRepId's on the Customer table
select distinct c.SupportRepId
from customer c

-- display the distinct EmployeeId's on the Employee table
select distinct em.employeeid
from employee em

-- Assume that the Customer.SupportRepId is mapped to the Employee.EmployeeId
-- Join the two tables 
-- Display the Customer countries and the SupportRepId that covers them
-- Order the columns in any way you seem fit
select c.country as customer_country
	, c.supportrepid
	, em.lastname as employee_ln
	, em.firstname as employee_fn
from employee em
inner join customer c on c.supportrepid = em.employeeid
order by c.country desc

-- what is the count of CustomerId's for each Customer country, Employee FirstName
-- we would like to know how many customers each rep has in each country


select c.country as customer_country
	, em.firstname as employee_fn
	, count(c.customerid) as number_of_customers
from employee em
inner join customer c on c.supportrepid = em.employeeid
group by customer_country, employee_fn
order by c.country desc


-- Display the Customer country, the Employee FirstName and calculate:
---- the number of customers in each country <<Hint:count(DISTINCT c.CustomerId)>>
---- the sum of the invoices Total
---- the average invoice per customer

-- Try to come up with another query to answer the business question, a small change to the above query is acceptable 

select 
    c.country as customer_country,
    em.firstname as employee_fn,
    count(distinct c.customerid) as number_of_unique_customers,
    sum(inv.total) as total_invoice_amount,
    sum(inv.total) / count(distinct c.customerid) as avg_invoice_per_customer
from employee em
inner join customer c 
    on c.supportrepid = em.employeeid
inner join invoice inv 
    on inv.customerid = c.customerid
group by 
    c.country,
    em.firstname
order by 
    avg_invoice_per_customer desc; -- most purchases per customer







