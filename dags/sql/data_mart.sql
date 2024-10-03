-- dijalankan manual
-- create schema dm;

-- create table dm.reff_date as
-- with date_series as (
-- select generate_series('2024-01-01'::date,'2024-12-31'::date,interval '1 day')::date as date
-- )
-- select date,extract(year from date)as year
-- ,case when length(extract(week from date)::text )= 1 then '0'||extract(week from date)::text 
-- else extract(week from date)::text end as week
-- ,concat(extract(year from date),case when length(extract(week from date)::text )= 1 then '0'||extract(week from date)::text 
-- else extract(week from date)::text end) as yearweek
-- from date_series;

-- Sampai sini

--trend revenue per week;
drop table if exists dm.trend_total_revenue_weekly;
create table if not exists dm.trend_total_revenue_weekly as 
with cte as (
select b.yearweek,rs.store_name,sum(amount) total_amount from raw.sales a
left join dm.reff_date b on a.date = b.date
left join raw.reff_store rs on a.store_id = rs.store_id
group by 1,2
)
select yearweek,store_name,total_amount,
coalesce (total_amount - lag(total_amount,1) over (partition by store_name order by yearweek),0) dod from cte;


--list_top_store;
drop table if exists dm.top_score;
create table if not exists dm.top_score as 
with cte as (select date,rs.store_name,sum(amount)total_amount from raw.sales  a
left join raw.reff_store rs on a.store_id = rs.store_id 
group by 1,2
order by 3 desc),cte_second as (
select *,row_number() over(partition by date order by total_amount desc) top from cte)
select date,store_name,total_amount from cte_second where top = 1 order by 1 asc;

--list_top_kasir;
drop table if exists dm.top_kasir;
create table if not exists dm.top_kasir as 
with cte as (select a.date,c.name,count(sale_id) jumlah,row_number() over(partition by a.date order by count(sale_id) desc) total_order
from raw.sales a left join raw.cashier c on a.cashier_id = c.cashier_id
group by 1,2)
select date,name,jumlah from cte where total_order = 1;

--trend and dod;
drop table if exists dm.trend_total_kasir_order;
create table if not exists dm.trend_total_kasir_order as
with cte as (select a.date,c.name,a.cashier_id,count(sale_id) jumlah_order from raw.sales a
left join raw.cashier c on a.cashier_id = c.cashier_id
group by 1,2,3
order by 1,2 asc)
select date,name,jumlah_order,coalesce (jumlah_order - lag(jumlah_order,1) over(partition by cashier_id order by date),0) as dod from cte; 


--trend revenue per date  per store untuk seluruh data;
drop table if exists dm.trend_total_revenue;
create table if not exists dm.trend_total_revenue as
with cte as (select date,rs.store_name,sum(amount) total_amount from raw.sales a
left join raw.reff_store rs on a.store_id = rs.store_id
group by 1,2)
select date,store_name,total_amount,coalesce (total_amount - lag(total_amount,1) over (partition by store_name order by date),0) dod from cte;

--list avg total penjualan per harinya, dan list max penjualan per harinya;
drop table if exists dm.total_banyak_order;
create table if not exists dm.total_banyak_order as
select date,round(avg(jumlah_penjualan),2) avg_penjualan_day,max(jumlah_penjualan)max_penjualan,min(jumlah_penjualan)min_penjualan from
(select date,store_id,count(sale_id) jumlah_penjualan from raw.sales
group by 1,2)b group by 1;

--list avg total_revenue per toko, list max penjualan per toko per harinya;
drop table if exists dm.total_revenue;
create table if not exists dm.total_revenue as
select date,avg(total_revenue) avg_revenue,max(total_revenue) max_revenue,min(total_revenue) min_revenue from (select date,store_id,sum(amount)total_revenue from raw.sales
group by 1,2)b
group by 1;