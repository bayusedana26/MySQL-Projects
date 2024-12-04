CREATE TABLE accounts (
id varchar(100) PRIMARY KEY,
name varchar(100) NOT NULL,
balance BIGINT NOT NULL
)

SELECT * FROM accounts

DESC accounts

SHOW CREATE TABLE accounts 

# Atomocity

START TRANSACTION

INSERT INTO accounts(id, name, balance)
	VALUES ('U001', 'Bayu Sedana', '1500000')
	
INSERT INTO accounts(id, name, balance)
	VALUES ('U002', 'Gale Sedana', '2000000')
	
COMMIT

START TRANSACTION

DELETE FROM accounts WHERE id = 'U001'

DELETE FROM accounts WHERE id = 'U002'

ROLLBACK 

# Consistency 

START TRANSACTION 

UPDATE accounts SET name = null 
	WHERE id = 'U001'
	
COMMIT 

SELECT * FROM accounts

# Isolation

START TRANSACTION 

SELECT * FROM accounts WHERE id IN ('U001', 'U002') FOR UPDATE 

UPDATE accounts SET balance = balance - 500000
	WHERE id = 'U001'
	
UPDATE accounts SET balance = balance + 500000
	WHERE id = 'U002'
	
COMMIT	

# Durability

START TRANSACTION 

SELECT * FROM accounts WHERE id IN ('U001', 'U002') FOR UPDATE 

UPDATE accounts SET balance = balance - 500000
	WHERE id = 'U001'
	
UPDATE accounts SET balance = balance + 500000
	WHERE id = 'U002'
	
COMMIT	

SELECT * FROM accounts






	
	