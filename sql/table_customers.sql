CREATE TABLE customers (
id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
email VARCHAR(100) NOT NULL UNIQUE KEY,
first_name VARCHAR(100) NOT NULL,
last_name VARCHAR(100) NOT NULL
)

DESC customers 

INSERT INTO
	customers (email, first_name,last_name)
VALUES 
('bayucoba2@mail.com', 'Bayu','Sedana2'),
('bayucoba3@mail.com', 'Bayu','Sedana3'),
('bayucoba4@mail.com', 'Bayu','Sedana4'),
('bayucoba5@mail.com', 'Bayu','Sedana5')

ALTER TABLE customers ADD CONSTRAINT email_unique UNIQUE (email)

SELECT * FROM customers

INSERT INTO customers (email, first_name, last_name)
VALUES		('bayu01@gmail.com', 'Bayu', 'Sedana01'),
			('bayu02@gmail.com', 'Bayu', 'Sedana02')