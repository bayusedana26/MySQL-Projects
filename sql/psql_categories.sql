create table categories 
(
	id varchar(10) primary key not null,
	name varchar(10) not null
)

insert into categories (id, name)
	values	('C001', 'Makanan'),
			('C002', 'Minuman')
			
select * from categories

insert into categories (id, name)
	values 	('C003', 'Gadget'),
			('C004', 'HP'),
			('C005', 'Kue')

select * from categories 
	inner join products on products.id_category = categories.id

select * from categories 
	left join products on products.id_category = categories.id
	
select * from categories 
	right join products on products.id_category = categories.id
	
 select * from categories 
	full join products on products.id_category = categories.id


