CREATE TABLE orders
(
id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
total INT NOT NULL,
order_date DATETIME DEFAULT CURRENT_TIMESTAMP
)

DESC orders

CREATE TABLE orders_detail 
(
id_product VARCHAR(20) NOT NULL,
id_order INT NOT NULL,
quantity INT NOT NULL,
price INT NOT NULL,
PRIMARY KEY (id_product, id_order)
)

DESC orders_detail 

ALTER TABLE orders_detail 
	ADD CONSTRAINT fk_orders_detail_product
	FOREIGN KEY (id_product) REFERENCES products(id)
	
ALTER TABLE orders_detail 
	ADD CONSTRAINT fk_orders_detail_orders
	FOREIGN KEY (id_order) REFERENCES orders(id)
	
SHOW CREATE TABLE orders_detail 

SELECT * FROM orders 

INSERT INTO orders (total)
VALUES ('7')

INSERT INTO orders_detail (id_product, id_order, quantity, price)
VALUES 	('P001', '1', '2', '36000'),
		('P002', '1', '2', '36000')
		
INSERT INTO orders_detail (id_product, id_order, quantity, price)
VALUES 	('P003', '2', '3', '60000'),
		('P004', '3', '4', '100000')
		
INSERT INTO orders_detail (id_product, id_order, quantity, price)
VALUES 	('P004', '2', '5', '120000'),
		('P003', '3', '2', '68000')
		
SELECT * FROM orders_detail 

SELECT * FROM orders as o
	JOIN orders_detail as od ON (o.id = od.id_order)
	JOIN products as p ON (p.id = od.id_product)

SELECT o.id, p.id, p.name, od.quantity, od.price FROM orders as o
	JOIN orders_detail as od ON (o.id = od.id_order)
	JOIN products as p ON (p.id = od.id_product)
	

CREATE TABLE numbers (
id INT NOT NULL PRIMARY KEY
)

INSERT INTO numbers (id)
VALUES (1), (2), (3), (4), (5)

SELECT * FROM numbers 

SELECT n1.id, n2.id, (n1.id * n2.id) 
FROM numbers as n1 CROSS JOIN numbers as n2 ORDER BY n1.id, n2.id





		
