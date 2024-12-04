CREATE TABLE wishlist 
(
id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
id_product VARCHAR(20) NOT NULL,
description TEXT,
CONSTRAINT fk_wishlist_product FOREIGN KEY (id_product) REFERENCES products (id)
)

DESC wishlist 

SHOW CREATE TABLE wishlist

SELECT * FROM wishlist 

ALTER TABLE wishlist 
	DROP CONSTRAINT fk_wishlist_product
	
ALTER TABLE wishlist 
	ADD CONSTRAINT fk_wishlist_product
		FOREIGN KEY (id_product) REFERENCES products (id)
		ON DELETE CASCADE ON UPDATE CASCADE
		
INSERT INTO wishlist (id_product, description)
 VALUES ('P001', 'Makanan Kesukaan')
 
 INSERT INTO wishlist (id_product, description)
 VALUES ('PXXX', 'Makanan Kesukaan')
 
 SELECT * FROM wishlist JOIN products ON (wishlist.id_product = products.id)
 
 SELECT wishlist.id, products.id, products.name, wishlist.description 
 	FROM wishlist JOIN products ON (wishlist.id_product = products.id)
 	
 SELECT
	w.id as id_wishlist, p.id as id_product, p.name, w.description
FROM wishlist AS w JOIN products AS p ON (w.id_product = p.id)

ALTER TABLE wishlist ADD COLUMN id_customer INT

ALTER TABLE wishlist 
	ADD CONSTRAINT fk_wishlist_customer
	FOREIGN KEY (id_customer) REFERENCES customers (id)
	
ALTER TABLE wishlist MODIFY id_customer INT after id_product

UPDATE wishlist 
	SET id_customer = 1
	WHERE id = 2
	
SELECT c.first_name, c.email, p.id, p.name, w.description 
	FROM wishlist as w
	JOIN products as p ON (w.id_product = p.id)
	JOIN customers as c ON (w.id_customer = c.id) 


