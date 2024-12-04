create table sellers 
(
	id serial primary key not null,
	email varchar(100) unique not null,
	name varchar(100) not null
)

insert into sellers (name, email)
	values 	('Galeri Olahraga', 'galeriolaharaga@mail.com'),
			('Galeri Buah', 'galeribuah@mail.com'),
			('Galeri Buku', 'galeribuku@mail.com'),
			('Galeri Baju', 'galeribaju@mail.com'),
			('Galeri foto', 'galeriofoto@mail.com')
			
select * from sellers 

create index sellers_name_index on sellers (name)

create index sellers_email_and_name_index on sellers (email, name)

create index sellers_id_and_name_index on sellers (id, name)

select * from sellers where id = 1

select * from sellers where name = 'Galeri Olahraga' or email = 'galerifoto@mail.com'

select * from sellers where id = 4 or name = 'Galeri Buah'




			