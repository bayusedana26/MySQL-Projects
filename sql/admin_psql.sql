create table admin (
	id  serial primary key not null,
	first_name varchar(100) not null,
	last_name varchar(100)
)

insert into admin (first_name, last_name)
	values 	('Bayu', 'Sedana'),
			('Fitri', 'Ana'),
			('Popow', 'Leo')
			
select * from "admin"

select currval(pg_get_serial_sequence('admin', 'id'))

select currval('admin_id_seq')

create sequence contoh_sequence

select nextval('contoh_sequence')

select currval('contoh_sequence')


