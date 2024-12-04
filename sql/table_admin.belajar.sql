CREATE table admin(
	id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
	first_name VARCHAR(100) NOT NULL,
	last_name VARCHAR(100) NOT NULL
)

DESC admin

INSERT INTO admin(first_name, last_name)
	VALUES
		('Bayu', 'Sedana'),
		('Kyema', 'Gale'),
		('Freedom', 'Wing')
		
SELECT * FROM admin

DELETE FROM admin WHERE id = 3

INSERT INTO admin(first_name, last_name)
	VALUES ('Freedom', 'Wing')
	
INSERT INTO admin(first_name, last_name)
	VALUES ('Free', 'Kurtz')
	
SELECT LAST_INSERT_ID() 





