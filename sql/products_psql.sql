create table products (
	id varchar(10) not null,
	name varchar(100) not null,
	description text,
	price int not null,
	quantity int not null default 0,
	created_at timestamp not null default current_timestamp
)

insert into products(id, name, price, quantity)
	values ('P001', 'Mie Ayam Originial', 15000, 100)
	
insert into products (id, name, description, price, quantity)
	values('P002', 'Mie Ayam Bakso', 'Mie ayam original dengan bakso', 20000, 100)
	
insert into products(id, name, price, quantity)
	values 	('P003', 'Mie Ayam Bakso Tahu', 18000, 70),
			('P004', 'Mie Yamin', 20000, 30),
			('P005', 'Mie Ayam Komplit', 25000, 10)

select * from products 

select id, name, price, quantity from products 

alter table products 
	add primary key (id)
	
select
	id,
	name,
	price,
	quantity
from
	products
where
	quantity = 0
	
select
	id,
	name,
	price,
	quantity
from
	products
where
	price <= 20000
	
create type PRODUCT_CATEGORY as enum ('Makanan', 'Minumana', 'Lain-lain')

alter table products 
	add column category PRODUCT_CATEGORY 
	
alter table products
	drop column category
	
update products 
	set category = 'Makanan'
	where id = 'P001'

update products 
	set category = 'Makanan'
	where id = 'P002'
	
update products 
	set category = 'Makanan'
	where id = 'P003'	
	
update products 
	set category = 'Makanan'
	where id = 'P004'
	
update products 
	set category = 'Makanan'
	where id = 'P005'
	
select * from products  

update products 
	set category = 'Makanan',
		description = 'Mie ayam original dengan bakso tahu'
	where id = 'P003'
	
update products
	set price = price + 2000
	where id = 'P003'
	
insert into products (id, name, price, category)
	values ('P009', 'Kumaha', 1000, 'Minuman')	
		
alter type PRODUCT_CATEGORY rename value 'Minumana' to 'Minuman'

delete from products where id = 'P009'

select id as kode, price as harga from products

select * from products where price >= 15000 or category = 'Minuman'

select * from products where category != 'Minuman'

insert into products (id, name, price, category)
	values	('P006', 'Es Teh Manis', 3000, 'Minuman'),
			('P007', 'Es Teh Tawar', 1500, 'Minuman'),
			('P008', 'Es Jeruk', 4000, 'Minuman'),
			('P009', 'Jeruk Hangat', 4000, 'Minuman')
	
select * from products where price <= 10000 or category = 'Makanan'

select
	*
from
	products
where
	(quantity > 100
		or category = 'Makanan')
	and price > 10000
	
select * from products where name ilike '%es%'

select * from products where description is null

select * from products where price between 10000 and 20000

select * from products where category in ('Makanan', 'Minuman')

select * from products order by price asc, id desc

select * from products where price > 0 order by price asc, id desc limit 5 offset 2

select distinct category from products

select category from products

select 10 + 10 as hasil

select id, price / 1000 as price_in_k from products order by id asc

select pi()

select power (10, 3)

select cos(10), sin(10), tan(10)

select id, name, power(quantity, 2) as quantity_power_2 from products

select id, lower(name), length(name), lower(description) from products 

select id, extract(year from created_at), extract(month from created_at) from products 

select
	id,
	category
from
	products 

select
	id,
	category,
	case
		category
		when 'Makanan' then 'Enak'
		when 'Minuman' then 'Segar'
		else 'Apa itu?'
	end as category_case
from
	products 
	
select id, price,
	case
		when price <= 15000 then 'Murah'
		when price <= 20000 then 'Mahal'
		else 'Murah banget'
	end as price_case
from 
	products 
	
select id, description from products 

select
	id,
	name,
	case
		when description is null then 'Kosong'
		else description
	end as description
from
	products
	
select count(id) from products 

select avg(price) from products 

select max(price) from products

select min(price) from products 

select
	category,
	count(id) as "Total Products"
from
	products
group by
	category
	
select
	category,
	avg(price) as "Rata-rata harga",
	min(price) as "Harga terendah",
	max(price) as "Harga Termahal"
from
	products
group by
	category
	
select
	category,
	count(id) as total
from
	products
group by
	category
having
	count(id) > 4	
	
select
	category,
	avg(price) as "Rata-rata harga",
	min(price) as "Harga terendah",
	max(price) as "Harga Termahal"
from
	products
group by
	category
having 
	avg(price) > 15000
	
alter table products 
	add constraint price_check check (price >= 1000)
	
alter table products 
	add constraint quantity_check check (quantity >= 0)
	
select * from products order by id asc

insert into products (id, name, price, category)
	values	('X110', 'HDH', 10000, 'Minuman'),
			('X011', 'Es Teh Tawar', 1500, 'Minuman'),
			('X012', 'Es Jeruk', 4000, 'Minuman'),
			('X013', 'Jeruk Hangat', 4000, 'Minuman')
			
delete from products where id in ('X110', 'X011', 'X012', 'X013')

select * from products where name ilike '%mie%'

select * from products where to_tsvector(name) @@ to_tsquery('mie')

select cfgname from pg_ts_config

create index products_name_search on products using gin (to_tsvector('indonesian', name))

create index products_description_search on products using gin (to_tsvector('indonesian', description))

select * from products where name @@ to_tsquery('ayam')

select * from products where description @@ to_tsquery('mie')

select * from products where name @@ to_tsquery('ayam & tahu')

select * from products where name @@ to_tsquery('mie | ayam') 

select * from products where name @@ to_tsquery('!bakso')

select * from products where name @@ to_tsquery('''mie ayam''')

insert into products (id, name, price, quantity, category)
	values 	('X001', 'xxx', 5000, 10, 'Minuman')
	
delete from products where id = 'X001'

select * from products

alter table products
	add column id_category varchar(10)
	
alter table products
	add constraint fk_product_category foreign key (id_category) references categories(id)
	
select * from products

update products set id_category = 'C001'
	where category = 'Makanan'
	
update products set id_category = 'C002'
	where category = 'Minuman'

alter table products 
	drop column category
	
select * from products as p join categories on p.id_category = categories.id


select * from products p 

insert into products (id, name, price, quantity)
	values	('X001', 'contoh barang 1', 5000, 50),
			('X002', 'contoh barang 2', 7500, 60)
			
select * from products p 
	where price > (select avg(price) from products)
	
select
	max(price)
from
	(
	select
		products.price as price
	from
		categories
	join products on
		products.id_category = categories.id) as Max
		

start transaction 

select * from products p 

update products 
	set description = 'Mie yamin ini enak sekali'
	where id = 'P004'
	
update products set quantity = 50
	where id = 'P004'
	
commit

start transaction 

select * from products p where id = 'P004' for update

rollback

start transaction 

select * from products p where id = 'P004' for update 

select * from products p where id = 'P005' for update 

rollback

create role bayu

create role sedana

drop role bayu

drop role sedana

alter role bayu login password 'bayucoba1'

alter role sedana login password 'sedanacoba1'

grant insert, update, select on all tables in schema public to bayu

grant usage, select, update on guestbook_id_seq to bayu

grant insert, update, select on customers to sedana

revoke insert, update, select on all tables in schema public from bayu

revoke usage, select, update on guestbook_id_seq from bayu

revoke insert, update, select on customers from sedana

create role bayu login password 'bayucoba1'

create role sedana login password 'sedanacoba1'

create database belajar_restore











			










			
	

	
	
	
	





			


			
	
