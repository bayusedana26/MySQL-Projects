select current_schema 

create table contoh.products (
	id serial primary key not null,
	name varchar(100) not null
)

select * from contoh.products

insert into contoh.products (name)
	values	('PS5'),
			('Xbox')
			
select * from contoh.products



