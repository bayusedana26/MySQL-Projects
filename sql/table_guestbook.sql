CREATE TABLE guestbook
(
id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
email VARCHAR(100),
title VARCHAR(200),
content TEXT
)

DROP TABLE guestbook 

CREATE TABLE guestbooks
(
id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
email VARCHAR(100),
title VARCHAR(200),
content TEXT
)

DESC guestbooks 

SELECT * FROM customers

INSERT INTO guestbooks(email, title, content)
VALUES 	('bayu01@mail.com', 'Oke', 'Jalan-jalan'),
		('bayu02@mail.com', 'Eko', 'Jalan-jalan'),
		('bayu03@mail.com', 'Woke', 'Jalan-jalan'),
		('bayu04@mail.com', 'Onde', 'Jalan-jalan'),
		('bayu05@mail.com', 'Oke', 'Jalan-jalan'),
		('bayu06@mail.com', 'Oke', 'Jalan-jalan'),
		('bayu07@mail.com', 'Oke', 'Jalan-jalan')
		
SELECT * FROM guestbooks

UPDATE guestbooks SET email = 'bayu07@mail.com' WHERE email = 'bayu06@mail.com' 

SELECT DISTINCT email FROM customers
UNION
SELECT DISTINCT email FROM guestbooks

SELECT DISTINCT email FROM customers
UNION ALL
SELECT DISTINCT email FROM guestbooks

INSERT INTO guestbooks(email, title, content)
VALUES 	('bayu01@mail.com', 'Kemana', 'Jalan-jalan'),
		('bayu02@mail.com', 'Bayu', 'Jalan-jalan'),
		('bayu03@mail.com', 'Aduh', 'Jalan-jalan'),
		('bayu04@mail.com', 'Pantai', 'Jalan-jalan'),
		('bayu05@mail.com', 'Oke', 'Jalan-jalan'),
		('bayu06@mail.com', 'Oke', 'Jalan-jalan'),
		('bayu07@mail.com', 'Oke', 'Jalan-jalan')
		
INSERT INTO guestbooks (email)
VALUES		('bayucoba2@mail.com'),
			('bayucoba3@mail.com')

SELECT
	emails.email,
	COUNT(emails.email)
FROM
	(SELECT email FROM customers
UNION ALL
	SELECT email FROM guestbooks) as emails
GROUP BY
	emails.email
	
SELECT DISTINCT email from guestbooks
	
SELECT DISTINCT email FROM customers
	WHERE email IN (SELECT DISTINCT email FROM guestbooks)

SELECT DISTINCT customers.email FROM customers
	INNER JOIN guestbooks ON (customers.email = guestbooks.email)

SELECT DISTINCT customers.email, guestbooks.email FROM customers
	LEFT JOIN guestbooks ON (customers.email = guestbooks.email) 
	WHERE guestbooks.email IS NULL

START TRANSACTION 

INSERT INTO guestbooks (email, title, content)
VALUES		('contoh1@mail.com', 'Contoh1','Contoh1'),
			('contoh2@mail.com', 'Contoh2', 'Contoh2'),
			('contoh3@mail.com', 'Contoh3', 'Contoh3')
			
SELECT * FROM guestbooks

COMMIT

START TRANSACTION

DELETE FROM guestbooks 

SELECT * FROM guestbooks

ROLLBACK


		
	