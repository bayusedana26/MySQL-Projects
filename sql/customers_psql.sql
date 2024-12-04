create table customers 
(
	id serial primary key not null,
	email varchar(100) unique not null,
	first_name varchar(100) not null,
	last_name varchar(100)
)

insert into customers(email, first_name, last_name)
	values	('bayucoba1@mail.com', 'Bayu', 'Sedana')
	
select * from customers

insert into customers(email, first_name, last_name)
	values	('budicoba1@mail.com', 'Budi', ''),
			('jokocoba1@mail.com', 'Joko', 'Tingkir')
			
alter table customers 
	drop constraint customers_email_key
	
alter table customers 
	add constraint email_unique unique (email)
	
insert into customers(email, first_name, last_name)
	values	('bundacoba1@mail.com', 'Bunda', ''),
			('dafacoba1@mail.com', 'Dafa', '')
	

	

