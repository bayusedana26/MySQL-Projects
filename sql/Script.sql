use penjualan

/* Insert */

INSERT INTO pelanggan(nama) VALUES 
    ("Bayu"),
    ("Fikri"),
    ("Rahmat");

 insert into pelanggan(nama) values
 ("Alfian"),
 ("Lia");

insert into pelanggan(nama) values
("Riyo"), 
("Najmul");

insert into pelanggan (id, nama) values
(6, "Riyo");
   
INSERT INTO pemasok(nama) VALUES 
    ("Khay"),
    ("Heru"),
    ("Siti")
    
INSERT INTO barang(nama, harga, id_pemasok) VALUES 
    ("Pepsodent", 14500, 1),
    ("Lifeboy", 24600, 2),
    ("Clear", 44500, 3)

INSERT INTO transaksi(id_barang, id_pelanggan, jumlah, total) VALUES 
    (1, 1, 1, 14500),
    (2, 2, 2, 49200),
    (3, 3, 3, 133500)
    
/* Select */
    
Select * from barang;

select * from pemasok;

select * from pelanggan;

select * from transaksi;

/* Delete */

delete from pelanggan where id = 6;

delete from pelanggan where id = 7;






