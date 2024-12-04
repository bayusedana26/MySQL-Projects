CREATE TABLE category 
(
id VARCHAR(10) NOT NULL PRIMARY KEY,
name VARCHAR(100) NOT NULL
)

DESC category

DROP TABLE category 

CREATE TABLE categories
(
id VARCHAR(10) NOT NULL PRIMARY KEY,
name VARCHAR(100) NOT NULL
)

DESC categories 

SHOW CREATE TABLE categories

INSERT INTO categories 
	VALUES 	('C001', 'Makanan'), 
			('C002', 'Minuman'),
			('C003', 'Lain-lain')
			
SELECT * FROM categories 

INSERT INTO categories 
	VALUES 	('C004', 'Oleh-oleh'), 
			('C005', 'Elektronik')

SELECT * FROM categories as c
	INNER JOIN products as p ON (c.id = p.id_category)

SELECT * FROM categories as c
	JOIN products as p ON (c.id = p.id_category)

SELECT * FROM categories as c
	LEFT JOIN products as p ON (c.id = p.id_category)
	
SELECT * FROM categories as c
	RIGHT JOIN products as p ON (c.id = p.id_category)
	
SELECT * FROM categories CROSS JOIN products



