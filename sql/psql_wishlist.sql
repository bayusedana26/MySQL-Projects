create table wishlist 
(
	id serial  primary key not null,
	id_product varchar(10) not null,
	description text,
	constraint fk_wishlist_products foreign key (id_product) references products(id)
)

insert into wishlist(id_product, description)
	values	('P001', 'Mie ayam kesukaan'),
			('P002', 'Mie favorit')
			
select * from wishlist 

alter table wishlist 
	drop constraint fk_wishlist_products
	
alter table wishlist
	add constraint fk_wishlist_products foreign key (id_product) references products(id)
		on delete cascade on update cascade
		
insert into wishlist (id_product, description)
	values 	('X001', 'Ini contoh cascade')
	
select * from wishlist
	join products on products.id = wishlist.id_product
	
select
	products.id,
	products.name,
	wishlist.description
from
	wishlist
join products on
	products.id = wishlist.id_product
	
	select
	p.id,
	p.name,
	w.description
from
	wishlist as w
join products as p on
	p.id = w.id_product
	
alter table wishlist 
	add column id_customer int
	
alter table wishlist
	add constraint fk_wishlist_customer foreign key (id_customer) references customers(id)
	
update wishlist 
	set id_customer = 1
		where id = 2
		
update wishlist 
	set id_customer = 3
		where id = 3
			
update wishlist 
	set id_customer = 4
		where id in (2,3)
		
select * from wishlist 

select c.email, p.id, p.name, w.description from wishlist as w
	join customers as c on c.id = w.id_customer
	join products as p on p.id = w.id_product
	

		

	
	



			