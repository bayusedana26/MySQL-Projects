CREATE TABLE wallet 
(
id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
id_customer INT NOT NULL,
balance INT NOT NULL DEFAULT 0,
UNIQUE KEY id_customer_unique (id_customer),
FOREIGN KEY fk_wallet_customer (id_customer) REFERENCES customers (id)
)

DESC wallet

INSERT INTO wallet (id_customer)
	VALUES (1), (4) 
	
SELECT * FROM wallet 

SELECT c.email, w.balance
FROM wallet as w JOIN customers as c ON (w.id_customer = c.id)