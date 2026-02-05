--Jinjia Peng SQL FIM 590 Fall HW 1 & 2


-- Assignment 1 


-- SELECT THE TOP 10 rows from the Artist table
SELECT *
FROM Artist
limit 10;

-- SELECT THE TOP 10 Names from the Artist table
SELECT Name
FROM Artist
LIMIT 10;


-- SELECT THE TOP 10 Names from the Artist table and order by Name descending
SELECT Name 
from Artist
ORDER BY Name Desc
Limit 10;

-- SELECT THE TOP 10 rows from the Invoice table
select *
from Invoice 
limit 10;

-- SELECT all the rows from the Invoice table where the BillingCountry is France
select *
from invoice 
where billingcountry = 'France'

-- Display the column with the primary keys on the Invoice table (fine to use the column name)
select invoiceid
from invoice;

-- SELECT all the rows from the Invoice table where the BillingCountry is not France
select *
from Invoice 
where billingcountry <> 'France';

-- Select the distinct BillingCity where the BillingCountry is France
select distinct billingcity
from Invoice 
where billingcountry = 'France';

-- Select the Invoice count of each BillingCity where the BillingCountry is France
select billingcity, count(*) as invoicecount
from invoice
where BillingCountry = 'France'
group by billingcity;

-- Select the number of bytes for each MediaTypeID on the Track table
select MediaTypeId, sum(bytes) as sumbytes
from track
group by mediatypeid;

-- Select the average number of bytes for each MediaTypeID on the Track table
select mediatypeid, avg(bytes) as avgbytes
from track
group by mediatypeid;

-- Select the min and max number of bytes for each MediaTypeID on the Track table
select mediatypeid, min(bytes) as minbytes, max(bytes) as maxbytes
from track
group by mediatypeid;

-- what is the difference between the max and min bytes for each MediaTypeID on the Track table
select mediatypeid, max(bytes) - min(bytes) as byterange
from track
group by mediatypeid;

-- order the above table by the range difference between the max and min bytes for each MediaTypeID on the Track table
select mediatypeid, max(bytes) - min(bytes) as byterange
from track
group by mediatypeid
order by byterange desc;

-- Select the tracks from the tracks table where the number of bytes is greater that 1,000,000
select *
from track
where bytes > 1000000;

-- display the Name from the Track table with the most bytes
select Name
from track
order by bytes DESC 
limit 1;
-- display the PlaylistID from the PlaylistTrack table with the most tracks
select playlistid, count(*) as trackcount
from PlaylistTrack
group by playlistid
order by trackcount desc
limit 1;







-- Assignment 2



---- PART 1 ----

-- SELECT THE TOP 10 rows from the Artist table
select *
from Artist 
limit 10;

-- ORDER BY the ArtistId in a DESC order and SELECT THE TOP 10 rows from the Artist table
select *
from Artist 
order by artistid desc
limit 10;

-- SELECT the DISTINCT PlaylistId's on the PlaylistTrack table
select distinct playlistid
from playlisttrack;

-- INNER JOIN the Playlist table onto the PlaylistTrack. The join should be on the PRIMARY KEY (PK) on the Playlist table
select *
from playlist as p
join playlisttrack as pt
	on p.playlistid = pt.playlistid;

-- INNER JOIN the Playlist, PlaylistTrack, and Track tables on their shared primary keys
select *
from playlist as p
join playlisttrack as pt
	on p.playlistid = pt.playlistid
join track as t
	on pt.trackid = t.trackid;
		
-- SELECT the PlaylistId's have Michael Jackson songs
SELECT DISTINCT
  p.PlaylistId
from playlist as p
join playlisttrack as pt
	on p.playlistid = pt.playlistid
join track as t
	on pt.trackid = t.trackid
join album as al
	on al.albumid = t.albumid
where t.composer = 'Michael Jackson'


-- Now join the Album table onto our Playlist, PlaylistTrack, and Track table query (used just above)
-- What is the title of the album with the Michael Jackson songs from our playlist?  Use DISTINCT and SELECT only the Album Title Column
SELECT distinct al.title
from playlist as p
join playlisttrack as pt
	on p.playlistid = pt.playlistid
join track as t
	on pt.trackid = t.trackid
join album as al
	on al.albumid = t.albumid
where t.composer = 'Michael Jackson'


-- Now join the Artist table onto our query from above
-- Which Artist has the most Tracks on the PlaylistTrack table?
-- Use COUNT() in the SELECT Statement
-- Use GROUP BY
-- Use ORDER BY COUNT() DESC
SELECT count(*),
		ar.artistid,
		ar.name
from playlisttrack as pt
join playlist as p
	on p.playlistid = pt.playlistid
join track as t
	on pt.trackid = t.trackid
join album as al
	on al.albumid = t.albumid
join artist as ar
	on ar.artistid = al.artistid
group by ar.artistid, ar.name
order by count(*) desc

-- Join the Genre table onto the query above using the GenreId
-- What is the SUM() of the Bytes and Milliseconds for each Genre?
-- Name the two aggregated columns 'Bytes_Total' and 'Milliseconds_Total'

SELECT
  ge.GenreId,
  ge.Name AS Genre_Name,
  SUM(d.Milliseconds) AS Milliseconds_Total,
  SUM(d.Bytes)        AS Bytes_Total
FROM (
  SELECT DISTINCT
    t.TrackId, t.GenreId, t.Bytes, t.Milliseconds
  FROM Playlist AS p
  JOIN PlaylistTrack AS pt ON p.PlaylistId = pt.PlaylistId
  JOIN Track AS t          ON t.TrackId    = pt.TrackId
) AS d
JOIN Genre AS ge ON ge.GenreId = d.GenreId
GROUP BY ge.GenreId, ge.Name
ORDER BY ge.GenreId;


-- Use the above query to display the number of Bytes per Millisecond for each Genre
-- Name the column 'Bytes_per_Millisecond'
-- ORDER the results in DESC Bytes per Millisecond 

SELECT
  g.GenreId,
  g.Genre_Name,
  g.Bytes_Total,
  g.Milliseconds_Total,
  1.0 * g.Bytes_Total / NULLIF(g.Milliseconds_Total, 0) AS Bytes_per_Millisecond
FROM (
  SELECT
    ge.GenreId,
    ge.Name AS Genre_Name,
    SUM(d.Milliseconds) AS Milliseconds_Total,
    SUM(d.Bytes)        AS Bytes_Total
  FROM (
    SELECT DISTINCT
      t.TrackId, t.GenreId, t.Bytes, t.Milliseconds
    FROM Playlist AS p
    JOIN PlaylistTrack AS pt ON p.PlaylistId = pt.PlaylistId
    JOIN Track AS t          ON t.TrackId    = pt.TrackId
  ) AS d
  JOIN Genre AS ge ON ge.GenreId = d.GenreId
  GROUP BY ge.GenreId, ge.Name
) AS g
ORDER BY Bytes_per_Millisecond DESC;




-- Update the above query to display the Bytes per Millisecond for each Artist in addition to the Genre 
-- Exclude the 'Comedy' Genre by using WHERE g.Name <> 
SELECT
  ge.GenreId,
  ge.Name  AS Genre_Name,
  ar.ArtistId,
  ar.Name  AS Artist_Name,
  SUM(d.Milliseconds) AS Milliseconds_Total,
  SUM(d.Bytes)        AS Bytes_Total,
  1.0 * SUM(d.Bytes) / NULLIF(SUM(d.Milliseconds), 0) AS Bytes_per_Millisecond
FROM (
  -- Deduplicate to one row per track (playlist appearances removed)
  SELECT DISTINCT
    t.TrackId,
    t.GenreId,
    t.Bytes,
    t.Milliseconds,
    al.ArtistId
  FROM Playlist       AS p
  JOIN PlaylistTrack  AS pt ON pt.PlaylistId = p.PlaylistId
  JOIN Track          AS t  ON t.TrackId     = pt.TrackId
  JOIN Album          AS al ON al.AlbumId    = t.AlbumId
) AS d
JOIN Artist AS ar ON ar.ArtistId = d.ArtistId
JOIN Genre  AS ge ON ge.GenreId  = d.GenreId
WHERE ge.Name <> 'Comedy'
GROUP BY ge.GenreId, ge.Name, ar.ArtistId, ar.Name
ORDER BY Bytes_per_Millisecond DESC;
	


---- PART 2 ----

-- Select top 10 rows from Invoice table
select *
from invoice;

-- Select all the rows from the Invoice table where the BillingCountry is Germany
select *
from invoice
where billingcountry = 'Germany';

-- Filter the table by Invoice Dates more recent than 2010
select *
from Invoice 
where invoicedate > '2010-01-01';

-- Inner join the Invoice table on the InvoiceLine table and the Track table
select *
from invoice as inv
join invoiceline as invl 
	on inv.invoiceid = invl.InvoiceId
join track as t
	on t.trackid = invl.trackid;


---- The above is the base query used to answer the following:
-- What German city has the largest average invoice
SELECT
  x.BillingCity,
  AVG(x.Total) AS avg_invoice
FROM (
  SELECT DISTINCT
    inv.InvoiceId, inv.BillingCity, inv.Total
  FROM Invoice AS inv
  INNER JOIN InvoiceLine AS invl ON inv.InvoiceId = invl.InvoiceId
  INNER JOIN Track       AS t    ON t.TrackId     = invl.TrackId
  WHERE inv.BillingCountry = 'Germany'
) AS x
GROUP BY x.BillingCity
ORDER BY avg_invoice DESC
LIMIT 1;



-- What is the most popular genre for each German city?
SELECT
  s.BillingCity,
  s.BillingCountry,
  s.GenreId,
  s.GenreName,
  s.items AS Items_Purchased
FROM (
  SELECT
    inv.BillingCity,
    inv.BillingCountry,
    ge.GenreId,
    ge.Name AS GenreName,
    SUM(invl.Quantity) AS items,
    ROW_NUMBER() OVER (
      PARTITION BY inv.BillingCity
      ORDER BY SUM(invl.Quantity) DESC, ge.GenreId
    ) AS rn
  FROM Invoice AS inv
  JOIN InvoiceLine AS invl ON inv.InvoiceId = invl.InvoiceId
  JOIN Track AS t          ON t.TrackId     = invl.TrackId
  JOIN Genre AS ge         ON ge.GenreId    = t.GenreId
  WHERE inv.BillingCountry = 'Germany'
  GROUP BY inv.BillingCity, inv.BillingCountry, ge.GenreId, ge.Name
) AS s
WHERE s.rn = 1
ORDER BY s.BillingCity;


-- Expand the query to include USA, Canada, and Argentina and rewrite the above two questions
-- City with the largest average invoice in each of the four countries
WITH one_inv AS (
  SELECT DISTINCT
    inv.InvoiceId,
    inv.BillingCountry,
    inv.BillingCity,
    inv.Total
  FROM Invoice AS inv
  WHERE inv.BillingCountry IN ('Germany','USA','Canada','Argentina')
),
avg_by_city AS (
  SELECT
    BillingCountry,
    BillingCity,
    AVG(Total) AS avg_invoice
  FROM one_inv
  GROUP BY BillingCountry, BillingCity
),
ranked AS (
  SELECT
    BillingCountry,
    BillingCity,
    avg_invoice,
    ROW_NUMBER() OVER (
      PARTITION BY BillingCountry
      ORDER BY avg_invoice DESC, BillingCity
    ) AS rn
  FROM avg_by_city
)
SELECT BillingCountry, BillingCity, avg_invoice
FROM ranked
WHERE rn = 1
ORDER BY BillingCountry;

-- Most popular genre for each city in those countries
WITH city_genre AS (
  SELECT
    inv.BillingCountry,
    inv.BillingCity,
    ge.GenreId,
    ge.Name AS GenreName,
    SUM(invl.Quantity) AS items
  FROM Invoice       AS inv
  JOIN InvoiceLine   AS invl ON invl.InvoiceId = inv.InvoiceId
  JOIN Track         AS t    ON t.TrackId      = invl.TrackId
  JOIN Genre         AS ge   ON ge.GenreId     = t.GenreId
  WHERE inv.BillingCountry IN ('Germany','USA','Canada','Argentina')
  GROUP BY inv.BillingCountry, inv.BillingCity, ge.GenreId, ge.Name
),
ranked AS (
  SELECT
    BillingCountry,
    BillingCity,
    GenreId,
    GenreName,
    items,
    ROW_NUMBER() OVER (
      PARTITION BY BillingCountry, BillingCity
      ORDER BY items DESC, GenreId
    ) AS rn
  FROM city_genre
)
SELECT
  BillingCountry,
  BillingCity,
  GenreId,
  GenreName,
  items AS Items_Purchased
FROM ranked
WHERE rn = 1
ORDER BY BillingCountry, BillingCity;




---- Logic Questions
-- What is the most popular song we invoice for?
select t.name,
		count(*)
from invoiceline as invl
join track as t
	on t.trackid = invl.trackid
group by t.Name
order by count(*) desc
limit 1;

-- What is the best selling album by country?
WITH AlbumCountryAgg AS (
  SELECT i.BillingCountry,
         a.Title AS AlbumTitle,
         COUNT(*) AS SalesCount
  FROM Invoice     AS i
  JOIN InvoiceLine AS il ON i.InvoiceId = il.InvoiceId
  JOIN Track       AS t  ON il.TrackId  = t.TrackId
  JOIN Album       AS a  ON t.AlbumId   = a.AlbumId
  GROUP BY i.BillingCountry, a.Title
),
AlbumCountryRanked AS (
  SELECT BillingCountry,
         AlbumTitle,
         SalesCount,
         ROW_NUMBER() OVER (
           PARTITION BY BillingCountry
           ORDER BY SalesCount DESC, AlbumTitle
         ) AS rn
  FROM AlbumCountryAgg
)
SELECT BillingCountry, AlbumTitle, SalesCount
FROM AlbumCountryRanked
WHERE rn = 1
ORDER BY BillingCountry;

-- What is the best selling album by genre?
WITH album_sales AS (
  SELECT
      ge.Name AS genre,
      a.AlbumId,
      a.Title AS AlbumTitle,
      COUNT(*) AS SalesCount
  FROM Invoice i
  JOIN InvoiceLine il ON i.InvoiceId = il.InvoiceId
  JOIN Track t        ON il.TrackId  = t.TrackId
  JOIN Album a        ON t.AlbumId   = a.AlbumId
  JOIN Genre ge       ON ge.GenreId  = t.GenreId
  GROUP BY ge.Name, a.AlbumId, a.Title
),
ranked AS (
  SELECT
      genre,
      AlbumTitle,
      SalesCount,
      ROW_NUMBER() OVER (
        PARTITION BY genre
        ORDER BY SalesCount DESC, AlbumTitle
      ) AS rn
  FROM album_sales
)
SELECT genre, AlbumTitle, SalesCount
FROM ranked
WHERE rn = 1
ORDER BY genre;


-- Assume that UnitPrice divided by Bytes is the revenue measure, what is the largest and smallest revenue invoice?
WITH RevenuePerInvoice AS (
  SELECT i.InvoiceId,
         SUM(il.UnitPrice / NULLIF(CAST(t.Bytes AS REAL), 0)) AS Revenue
  FROM Invoice     AS i
  JOIN InvoiceLine AS il ON i.InvoiceId = il.InvoiceId
  JOIN Track       AS t  ON il.TrackId  = t.TrackId
  GROUP BY i.InvoiceId
),
Largest AS (
  SELECT 'Largest' AS Bucket, InvoiceId, Revenue
  FROM RevenuePerInvoice
  ORDER BY Revenue DESC
  LIMIT 1
),
Smallest AS (
  SELECT 'Smallest' AS Bucket, InvoiceId, Revenue
  FROM RevenuePerInvoice
  ORDER BY Revenue ASC
  LIMIT 1
)
SELECT * FROM Largest
UNION ALL
SELECT * FROM Smallest;