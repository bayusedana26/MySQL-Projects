CREATE TABLE sellers (
	ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	name VARCHAR(100) NOT NULL,
	email VARCHAR(100) NOT NULL,
	UNIQUE KEY email_unique (email),
	INDEX name_index (name)	
)

DESC sellers 

SHOW CREATE TABLE sellers 

SELECT * FROM sellers
