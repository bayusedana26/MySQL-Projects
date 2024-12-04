create table wallet 
(
	id serial primary key not null,
	id_customer int unique not null,
	balance int not null default 0,
	constraint fk_wallet_customer foreign key (id_customer) references customers(id)
)

insert into wallet (id_customer, balance)
	values 	(1, 2500000),
			(3, 5000000),
			(4, 7500000),
			(5, 10000000),
			(6, 12500000)
			
select * from wallet 

insert into wallet (id_customer, balance)
	values	(2, 300000)
	
select c.email, c.first_name, w.balance from wallet as w 
	join customers as c on w.id_customer = c.id
	
select * from customers join wallet on wallet.id_customer = customers.id
