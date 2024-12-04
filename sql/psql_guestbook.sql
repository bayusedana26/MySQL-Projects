create table guestbook (
	id serial primary key not null,
	email varchar(100) not null,
	title varchar(100) not null,
	content text
)

insert into guestbook (email, title, content)
	values 	('bayucoba1@mail.com', 'Feedback bayu', 'ini feedback bayu'),
			('budicoba1@mail.com', 'Feedback budi', 'ini feedback budi'),
			('kumaha1@mail.com', 'Ini kumaha ya', 'Ini teh kunaon eeeeuy'),
			('bayucoba1@mail.com', 'Feedback bayu', 'ini feedback bayu'),
			('budicoba1@mail.com', 'Feedback budi', 'ini feedback budi'),
			('kumaha1@mail.com', 'Ini kumaha ya', 'Ini teh kunaon eeeeuy'),
			('bundacoba1@mail.com', 'Feedback bunda', 'ini feedback bunda'),
			('jokocoba1@mail.com', 'Feedback joko', 'ini feedback joko'),
			('dafa1@mail.com', 'Ini kumaha ya si dafa', 'Ini teh kunaon dafa')
			
select * from guestbook 

select distinct email from customers c 
	union
select distinct email from guestbook 

select distinct email from customers c 
	union all
select distinct email from guestbook 

select
	distinct email,
	count(email)
from
	(
	select
		distinct email
	from
		customers c
union all
	select
		distinct email
	from
		guestbook) as contoh
group by
	email
	
select email from customers c 
	intersect
select email from guestbook 

select email from customers c 
	except
select email from guestbook 

start transaction

insert into guestbook (email, title, content)
	values 	('transcoba1@mail.com', 'Feedback trans', 'ini feedback trans')
	
insert into guestbook (email, title, content)
	values 	('transcoba1@mail.com', 'Feedback trans', 'ini feedback trans2')
	
insert into guestbook (email, title, content)
	values 	('transcoba1@mail.com', 'Feedback trans', 'ini feedback trans3')
	
insert into guestbook (email, title, content)
	values 	('transcoba1@mail.com', 'Feedback trans', 'ini feedback trans4')
	
insert into guestbook (email, title, content)
	values 	('transcoba1@mail.com', 'Feedback trans', 'ini feedback trans5')
	
select * from guestbook g 

commit

start transaction

insert into guestbook (email, title, content)
	values 	('transcoba1@mail.com', 'Feedback trans', 'ini feedback rollback')
	
insert into guestbook (email, title, content)
	values 	('transcoba1@mail.com', 'Feedback trans', 'ini feedback roll2')
	
insert into guestbook (email, title, content)
	values 	('transcoba1@mail.com', 'Feedback trans', 'ini feedback roll3')
	
insert into guestbook (email, title, content)
	values 	('transcoba1@mail.com', 'Feedback trans', 'ini feedback roll4')
	
insert into guestbook (email, title, content)
	values 	('transcoba1@mail.com', 'Feedback trans', 'ini feedback roll5')
	
select * from guestbook g 

rollback
	
create schema contoh

drop schema contoh

set search_path to contoh

select current_schema 

set search_path to public

select * from public.products as p join public.categories c on p.id_category = c.id order by p.id









	
