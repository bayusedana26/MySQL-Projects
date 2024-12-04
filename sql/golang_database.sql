CREATE TABLE Customer (
id VARCHAR(100) NOT NULL,
name VARCHAR(100) NOT NULL,
PRIMARY KEY(id)
)

ALTER TABLE Customer Add Email varchar(100)

ALTER TABLE Customer Add birthdate time

ALTER TABLE Customer Add Balance int

ALTER TABLE Customer Add Rating float

ALTER TABLE Customer Add CreatedAt time

ALTER TABLE Customer Add Married Bool

SELECT * FROM Customer WHERE id = 'bayu' 