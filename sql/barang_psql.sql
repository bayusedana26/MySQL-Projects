select * from pg_tables where schemaname = 'public';

create table barang 
(
	code INT,
	name varchar(100),
	harga INT,
	jumlah INT
)

alter table barang 
	add column description text
	
alter table barang 
	drop column description
	
alter table barang 
	rename column name to full_name

truncate barang;

drop table barang;

create table barang 
(
	code INT not null,
	name varchar(100) not null,
	harga INT not null default 1000,
	jumlah INT not null,
	waktu timestamp not null default current_timestamp
)


