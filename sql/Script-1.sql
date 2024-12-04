/* Insert */

INSERT INTO pelanggan(nama) VALUES 
    ("Irvan"),
    ("Riyo"),
    ("Bayu"),
    ("Alfian");
   ("Bayu ")
   
   INSERT INTO pemasok(nama) VALUES 
    ("Ebit"),
    ("Fikri"),
    ("Najmul"),
    ("Rahmat");
   
   INSERT INTO barang(nama, harga, id_pemasok) VALUES 
    ("Nugget", 14500, 1),
    ("Chicken", 24600, 2),
    ("Sausage", 44500, 3),
    ("Dimsum", 20000, 4);
   
   INSERT INTO transaksi(id_barang, id_pelanggan, jumlah, total) VALUES 
    (1, 1, 1, 14500),
    (2, 2, 2, 49200),
    (3, 3, 3, 133500),
    (4, 4, 4, 80000)
    
    /* Select */
    
    SELECT * from pelanggan p 
    
    select * from pemasok pem
    
    select * from barang b 
    
    select * from transaksi t 
    
    /* Join */
    
    Select t.id, p.nama as nama_pelanggan, pem.nama as nama_pemasok, b.nama as nama_barang, b.harga as harga_satuan, t.jumlah, t.total, t.waktu 
	from transaksi t
    join barang b on t.id_barang = b.id 
    join pelanggan p on t.id_pelanggan = p.id 
    join pemasok pem on b.id_pemasok = pem.id 
    order by t.id 
    
    



