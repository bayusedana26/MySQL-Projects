create table orders (
	id serial primary key not null,
	total int not null,
	order_date timestamp not null default current_timestamp
)

create table orders_detail (
	id_product varchar(10) not null,
	id_order int not null,
	price int not null,
	quantity int not null,
	primary key (id_product, id_order)
)

alter table orders_detail 
	add constraint fk_orders_detail_product foreign key (id_product) references products(id)
	
alter table orders_detail 
	add constraint fk_orders_detail_order foreign key (id_order) references orders(id)
	
insert into orders (total)
	values 	(1),
			(1),
			(1)
			
select * from orders 

insert into orders_detail (id_product, id_order, price, quantity)
	values	('P001', 1, 150000, 2),
			('P002', 1, 260000, 3),
			('P003', 1, 712000, 4)
			
select * from orders_detail 

insert into orders_detail (id_product, id_order, price, quantity)
	values	('P004', 2, 150000, 2),
			('P005', 2, 260000, 3),
			('P006', 2, 712000, 4)
			
insert into orders_detail (id_product, id_order, price, quantity)
	values	('P002', 3, 150000, 2),
			('P001', 3, 260000, 3),
			('P004', 3, 712000, 4)
			
select * from orders 
	join orders_detail as od on od.id_order = orders.id
	join products as p on od.id_product = p.id
			
			
select * from orders 
	join orders_detail as od on od.id_order = orders.id
	join products as p on od.id_product = p.id
	where orders.id = 1

			


