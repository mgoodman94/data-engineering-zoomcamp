-- total trips on 1/15
select count(*)
from public.yellow_taxi_data
where to_char("tpep_pickup_datetime", 'mon-dd') = 'jan-15';

-- max tip per trip date
select date("tpep_pickup_datetime"), max("tip_amount") as MaxTip
from public.yellow_taxi_data
group by date("tpep_pickup_datetime")
order by MaxTip desc;

-- most popular destination for people picked up in Central Park on 1/14
select coalesce(d."Zone", 'Unknown') as "DropOffZone", count(*) as "DropOffCount"
from public.yellow_taxi_data as ytd 
inner join public.taxi_zone as p on p."LocationID" = ytd."PULocationID"
inner join public.taxi_zone as d on d."LocationID" = ytd."DOLocationID"
where to_char(ytd."tpep_pickup_datetime", 'mon-dd') = 'jan-14'
	and p."Zone" = 'Central Park'
group by d."Zone"
order by "DropOffCount" desc;

-- most expensive pickup-dropoff pair
select concat(coalesce(p."Zone", 'Unknown'), '/', coalesce(d."Zone", 'Unknown')) as PickupDropoff, 
	round(cast(avg(ytd.total_amount) as numeric), 2) as "AverageCost"
from public.yellow_taxi_data as ytd 
inner join public.taxi_zone as p on p."LocationID" = ytd."PULocationID"
inner join public.taxi_zone as d on d."LocationID" = ytd."DOLocationID"
group by concat(coalesce(p."Zone", 'Unknown'), '/', coalesce(d."Zone", 'Unknown'))
order by "AverageCost" desc;