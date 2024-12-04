DESC barang

SHOW CREATE TABLE barang

ALTER TABLE barang 
ADD COLUMN brand text

ALTER TABLE barang 
ADD COLUMN salah text

ALTER TABLE barang 
DROP COLUMN salah

ALTER TABLE barang 
MODIFY nama VARCHAR(200) AFTER deskripsi

ALTER TABLE barang 
MODIFY nama VARCHAR(200) FIRST

ALTER TABLE barang 
MODIFY id_barang INT NOT NULL

ALTER TABLE barang 
MODIFY nama VARCHAR(200) NOT NULL

ALTER TABLE barang 
MODIFY jumlah INT NOT NULL DEFAULT 0

ALTER TABLE barang 
MODIFY harga INT NOT NULL DEFAULT 0

ALTER TABLE barang 
ADD waktu_dibuat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP

INSERT INTO barang (id_barang, nama)
VALUES ('15', "Mangga")

ALTER TABLE barang 
DROP COLUMN deskripsi

ALTER TABLE barang 
DROP COLUMN brand

SELECT * FROM barang b

TRUNCATE barang 

SHOW tables

DROP table barang 

CREATE TABLE products (
id_product VARCHAR(20) PRIMARY KEY NOT NULL,
name VARCHAR(100) NOT NULL,
description TEXT,
price INT UNSIGNED NOT NULL,
quantity INT UNSIGNED NOT NULL DEFAULT 0,
created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)

DESC products

SHOW CREATE TABLE products

SELECT * FROM products

INSERT INTO products(id_product, name, price, quantity)
VALUES ('P001', 'Mie Ayam Original', '15000', '100')

INSERT INTO products(id_product, name, description, price, quantity)
VALUES ('P002', 'Mie Ayam Bakso', 'Mie Ayam + Bakso', 18000, 70)

INSERT INTO products(id_product, name, price, quantity)
VALUES 
('P003', 'Mie Ayam Yamin', 20000, 80),
('P004', 'Mie Ayam Udang', 25000, 50),
('P005', 'Mie Ayam Udang', 25000, 30)

INSERT INTO products(id_product, name, price, quantity)
VALUES 
('P006', 'Mie Ayam Ceker', 18000, 10),
('P007', 'Mie Ayam Kerang', 18000, 15),
('P008', 'Mie Ayam Ungekp', 18000, 25)

SELECT id_product, name, price, quantity FROM products 

SELECT * FROM products WHERE quantity = 100

SELECT * FROM products WHERE price = 18000

ALTER TABLE products
ADD COLUMN category enum ("Makanan", "Minuman", "Lain-lain") AFTER name

UPDATE products
SET category = 'Makanan'
where id_product = 'p001'

UPDATE
	products
SET
	category = 'Makanan',
	description = 'Mie Ayam + Yamin'
where
	id_product = 'p003'

UPDATE
	products
SET
	price = price + 3000
where
	id_product = 'p001'

DELETE FROM products WHERE id_product = 'p007'

SELECT
	id_product as id,
		name as nama,
		price as harga,
		quantity as jumlah
FROM
	products

SELECT
	p.id_product as id,
		p.name as nama,
		p.price as harga,
		p.quantity as jumlah
FROM
	products as p
	
SELECT * FROM products WHERE price <= 22000	

SELECT * FROM products WHERE quantity <= 50

SELECT * FROM products WHERE category != 'Minuman'

SELECT
	id_product,
	name,
	price,
	quantity
FROM
	products
WHERE
	price > 10000
	AND quantity <= 50
	
SELECT
	*
FROM
	products
WHERE
	(category = 'Makanan'
		OR quantity > 20)
	AND price < 20000
	
SELECT * FROM products WHERE name LIKE '%Ayam%'

SELECT * FROM products WHERE name LIKE  '%Bakso'

UPDATE products set name = 'Mie Ayam Ungkep' WHERE id_product = 'P008'

UPDATE products set category ='Makanan' WHERE id_product  = 'P002'

SELECT * FROM products p WHERE category  IS NULL 

SELECT * FROM products p WHERE description IS NOT NULL 

SELECT * FROM products WHERE price BETWEEN 10000 AND 20000

SELECT * FROM products WHERE price NOT BETWEEN 10000 AND 20000

INSERT INTO products(id_product, name, category, price, quantity)
	VALUES
	('P009', 'Es Teh', 'Minuman', 3000, 10),
	('P010', 'Es Jeruk', 'Minuman', 5000, 15),
	('P011', 'Es Campur', 'Minuman', 8000, 17),
	('P012', 'Soda Gembira', 'Minuman', 10000, 20)
	
INSERT INTO products(id_product, name, category, price, quantity)
	VALUES
	('P013', 'Lontong', 'Lain-lain', 2000, 100),
	('P014', 'Kerupuk Putih', 'Lain-lain', 1500, 200),
	('P015', 'Kacang', 'Lain-lain', 2000, 150),
	('P016', 'Kerupuk Udang', 'Lain-lain', 2000, 200)
	
SELECT * FROM products p 

SELECT * FROM products WHERE category IN ('Minuman', 'Makanan')

SELECT * FROM products WHERE category NOT IN ('Minuman', 'Makanan')

SELECT id_product, name, price, category FROM products WHERE category  is NOT NULL ORDER BY category, price DESC

SELECT * FROM products ORDER BY id_product LIMIT 3, 5

SELECT category FROM products p 

SELECT DISTINCT category FROM products p 

SELECT 10 * 10 as hasil

SELECT id, name, price DIV 1000 as 'Price in K' FROM products p 

SELECT id, COS(price), SIN(price), TAN(price) FROM products p 

SELECT name, price from products p WHERE price DIV 1000 > 7

SELECT
	id_product,
	LOWER(name) as 'name lower',
	UPPER(name) as 'name upper', 
	LENGTH(name) as 'name lenght'
FROM
	products
	
SELECT
	id_product,
	created_at,
	EXTRACT(YEAR FROM created_at) as Year,
	EXTRACT(MONTH FROM created_at) as MONTH
FROM
	products p 

SELECT
	id_product,
	created_at,
	YEAR(created_at),
	MONTH(created_at)
FROM
	products
	
SELECT
	id_product,
	category,
	CASE
		category
		WHEN
		'Makanan' THEN 'Enak'
		WHEN 
		'Minuman' THEN 'Segar'
		WHEN 
		'Lain-lain' THEN 'LUMAYAN'
	END AS 'new category'
FROM
	products
WHERE
	category IS NOT NULL
	
SELECT
	id_product,
	price,
	IF(price <= 15000,
	'Murah',
	IF(price <= 20000,
	'Mahal',
	'Mahal Banget')) as 'Kategori harga'
FROM
	products p

SELECT
	id_product,
	name,
	IFNULL(description, 'Kosong')
FROM
	products p
	
SELECT COUNT(name) as 'Total Data nama' FROM products p 

SELECT MAX(price) as 'Menu termahal' FROM products p 

SELECT MIN(price) as 'Menu termurah' FROM products p 

SELECT AVG(price) as 'Rata=rata harga menu' FROM products p 

SELECT SUM(quantity) as 'Stok barang' FROM products p 

ALTER TABLE products RENAME COLUMN id_product TO id

SELECT * FROM products

UPDATE products SET category = 'Makanan' WHERE id = 'p008'

SELECT
	category,
	COUNT(id) as 'Total produk'
FROM
	products
GROUP BY
	category

SELECT
	category,
	MAX(price) as 'Harga menu termahal'
FROM
	products p
GROUP BY
	category 
	
SELECT
	category,
	COUNT(id) as total
FROM
	products
GROUP BY
	category
HAVING
	total > 2
	
SELECT * FROM products p 
	
INSERT INTO products (id, name, category, price, quantity)
VALUES
	('p017', 'Kopiko', 'Lain-lain', 500, 1000)
	
UPDATE products SET id = 'P017' WHERE name = 'Kopiko'

UPDATE products SET price = 1000 WHERE name = 'Kopiko'

ALTER TABLE products 
	ADD CONSTRAINT price_check CHECK ( price >= 1000)
	
SHOW CREATE TABLE products 

INSERT INTO products (id, name, category, price, quantity)
VALUES
	('P018', 'Relaxa', 'Lain-lain', 500, 1500)
	
ALTER TABLE products
	ADD FULLTEXT product_fulltext (name, description)
	
SHOW CREATE TABLE products 

SELECT * FROM products WHERE name LIKE '%Ayam%' OR description LIKE '%Ayam%'

SELECT * FROM products WHERE MATCH(name, description) 
	AGAINST ('ayam' IN NATURAL LANGUAGE MODE)

SELECT * FROM products WHERE MATCH(name, description) 
	AGAINST ('+ayam -bakso' IN BOOLEAN MODE)
	
SELECT * FROM products WHERE MATCH(name, description) 
	AGAINST ('bakso' WITH QUERY EXPANSION)
	
INSERT INTO products (id, name, category, price, quantity)
VALUES ('pxxx', 'Tas Belanja', 'Lain-lain', '1500', '2000')

UPDATE products SET id  = 'Pxxx' WHERE name = 'Tas Belanja'

SELECT * FROM products p 

DELETE FROM products WHERE id = 'Pxxx'

ALTER TABLE products DROP COLUMN category

ALTER TABLE products ADD COLUMN id_category VARCHAR(10)

ALTER TABLE products ADD CONSTRAINT fk_id_categories 
	FOREIGN KEY (id_category) REFERENCES categories (id)
	
DESC products 

ALTER TABLE products MODIFY id_category VARCHAR(10) AFTER name

SHOW CREATE TABLE products 

SELECT * FROM products p 

UPDATE products 
	SET id_category = 'C001' 
	WHERE id in ('P001', 'P002', 'P003', 'P004', 'P005', 'P006', 'P008')

UPDATE products 
	SET id_category = 'C002' 
	WHERE id in ('P009', 'P010', 'P011', 'P012')
	
UPDATE products 
	SET id_category = 'C003' 
	WHERE id in ('P013', 'P014', 'P015', 'P016')
	
UPDATE products 
	SET id_category = 'C003' 
	WHERE id in ('P017')

SELECT p.id, p.name, c.name
FROM products as p
JOIN categories as c ON (p.id_category = c.id)

INSERT INTO products (id, name, price, quantity)
	VALUES 	('X001', 'X Satu', 1000, 30),
			('X002', 'X Dua', 1500, 15),
			('X003', 'X Tiga', 7500, 75)
			
SELECT * FROM products p

SELECT AVG(price) FROM products

SELECT * FROM products
	WHERE price > (SELECT AVG(price) FROM products)
	
SELECT MAX(price) FROM products

UPDATE products SET price = 1000000 WHERE id = 'X003'

SELECT price FROM categories 
	JOIN products ON (products.id_category = categories.id)

SELECT
	MAX(cp.price)
FROM
	(SELECT price FROM categories
	JOIN products ON (products.id_category = categories.id)) as cp


	



	


