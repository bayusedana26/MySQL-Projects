SELECT * FROM diskuk_od 

SHOW CREATE TABLE diskuk_od 

ALTER TABLE diskuk_od RENAME umkm_jabar

SELECT * FROM umkm_jabar 

SHOW CREATE TABLE umkm_jabar 

SELECT * FROM umkm_jabar WHERE nama_kabupaten_kota = 'KOTA BANDUNG'

SELECT * FROM umkm_jabar WHERE tahun >= 2019 ORDER BY jumlah_umkm

SELECT DISTINCT kategori_usaha FROM umkm_jabar

SELECT * FROM umkm_jabar WHERE kategori_usaha IN ('FASHION', 'MAKANAN')

SELECT * FROM umkm_jabar WHERE kategori_usaha = 'FASHION' OR kategori_usaha = 'MAKANAN'

SELECT * FROM umkm_jabar uj WHERE kategori_usaha = 'FASHION' AND nama_kabupaten_kota  = 'KABUPATEN KARAWANG' 

SELECT * FROM umkm_jabar uj WHERE kategori_usaha NOT IN ('MAKANAN', 'MINUMAN', 'KULINER')

SELECT
	nama_kabupaten_kota,
	kategori_usaha,
	jumlah_umkm,
	satuan,
	tahun
FROM
	umkm_jabar uj
WHERE
	nama_kabupaten_kota = 'KABUPATEN TASIKMALAYA'
	AND kategori_usaha = 'BATIK'
	AND tahun >= 2018
	AND tahun <= 2020
	
# Di Kota Bandung, Kabupaten Bandung dan Kabupaten Bandung Barat, dimanakah yang paling banyak punya UMKM Kuliner di tahun 2021
	
SELECT 
	*
FROM
	umkm_jabar uj
WHERE
	nama_kabupaten_kota LIKE '%BANDUNG%'
	AND kategori_usaha = 'KULINER'
	AND tahun = 2021
	
# Angka 7 di digit ke 3
	
SELECT * FROM umkm_jabar uj WHERE kode_kabupaten_kota LIKE '__7%'
	
SELECT COUNT(*) FROM umkm_jabar uj  

# Jumlah umkm di bekasi tahun 2017
SELECT
	nama_kabupaten_kota,
	SUM(jumlah_umkm) as jumlah_umkm_bekasi,
	tahun
FROM
	umkm_jabar
WHERE
	nama_kabupaten_kota = 'KABUPATEN BEKASI'
	AND tahun = 2017

# Tren jumlah umkm di kabupaten karawang dari tahun 2017 - 2021
SELECT
	nama_kabupaten_kota,
	tahun,
	SUM(jumlah_umkm)
FROM
	umkm_jabar uj
WHERE
	nama_kabupaten_kota = 'KABUPATEN KARAWANG'
	AND tahun >= 2017
	AND tahun <= 2021
GROUP BY
	tahun  
	
# Rata2 jumlah umkm tiap kategori usaha di tiap kabupaten / kota tahun 2021
SELECT
	kategori_usaha,
	tahun,
	AVG(jumlah_umkm) as rata2_jumlah_umkm
FROM
	umkm_jabar uj
WHERE
	tahun = 2021
GROUP BY
	kategori_usaha,
	tahun
ORDER BY 
	kategori_usaha 

# Nilai minimum dan maximum di kolom jumlah umkm
SELECT
	MIN(jumlah_umkm) as min_jumlah_umkm,
	MAX(jumlah_umkm) as max_jumlah_umkm
FROM
	umkm_jabar uj 

# Kabupaten / kota yang punya umkm kurang dari 100,000 tahun 2020
SELECT
	nama_kabupaten_kota,
	SUM(jumlah_umkm) as jumlah_umkm_2020,
	tahun
FROM
	umkm_jabar
WHERE
	tahun = 2020
GROUP BY
	nama_kabupaten_kota
HAVING
	jumlah_umkm_2020 <= 100000

-- Membuat stored procedure sederhana get All
CREATE PROCEDURE getAllData()
BEGIN
    SELECT * FROM umkm_jabar;
END

CALL getAllData()

-- sp untuk pilih nama kabupaten kota
CREATE PROCEDURE selectCity(IN nama_daerah VARCHAR(50))
BEGIN
	SELECT * FROM umkm_jabar WHERE nama_kabupaten_kota = nama_daerah;
END

CALL selectCity('KABUPATEN BEKASI')

-- sp param out
CREATE PROCEDURE getTotalRow(OUT jumlah_row INT)
BEGIN
	SELECT COUNT(*) INTO jumlah_row FROM umkm_jabar;
END

CALL getTotalRow(@jumlah_row)

SELECT @jumlah_row

-- sp param in out
CREATE PROCEDURE getSelectedTotalRow(INOUT kode_kab INT)
BEGIN
	SELECT COUNT(*) INTO kode_kab FROM umkm_jabar WHERE kode_kabupaten_kota = kode_kab; 
END

SET @kode_daerah = 3216

CALL getSelectedTotalRow(@kode_daerah)

SELECT @kode_daerah

SELECT COUNT(*) FROM umkm_jabar uj WHERE kode_kabupaten_kota = 3216

-- sp untuk looping
CREATE TABLE test_looping (
	id_looping INT
)

CREATE PROCEDURE insert_id()
BEGIN
    DECLARE counter INT;
    SET counter = 1;

    WHILE counter <= 10 DO
        INSERT INTO test_looping(id_looping) VALUES (counter);
        SET counter = counter + 1;
    END WHILE;
END

CALL insert_id 

INSERT INTO test_looping (id_looping)
	VALUES	(1),
			(2),
			(3)

SELECT * FROM test_looping

UPDATE test_looping 
	SET id_looping = NULL 
	
-- sp if then
CREATE PROCEDURE test_if(IN bilangan INT, OUT hasil VARCHAR(100))
BEGIN
	IF bilangan >= 50 THEN SET hasil = "Lebih atau sama dengan 50";
	ELSE SET hasil = "Kurang dari 50";
END IF;
END

CALL test_if(30, @hasil)

SELECT IFNULL(@bilangan, 30), @hasil

-- check sp status
SHOW PROCEDURE STATUS

DROP PROCEDURE IF EXISTS getAllData

DROP PROCEDURE IF EXISTS selectCity

DROP PROCEDURE IF EXISTS getTotalRow

DROP PROCEDURE IF EXISTS getSelectedTotalRow

DROP PROCEDURE IF EXISTS insert_id

















